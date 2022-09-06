package engines

import (
	"strconv"
	"sync"
	"testing"
	"time"
)

func Test_Basic(t *testing.T) {
	engine := NewUint64Engine()

	txn1 := engine.NewTxn()
	_ = engine.Put(txn1, 30, "30")

	txn2 := engine.NewTxn()
	val, err := engine.Get(txn2, 30)
	if err == nil {
		t.Errorf("Expect err, got %v\n", val)
	}
	txn2.Commit()
	txn1.Commit()

	txn3 := engine.NewTxn()
	val, err = engine.Get(txn3, 30)
	if val != "30" {
		t.Errorf("Expect 30, got %v (err=%v)\n", val, err)
	}
	txn3.Commit()
}

func Test_Basic2(t *testing.T) {
	engine := NewUint64Engine()

	txn1 := engine.NewTxn()
	_ = engine.Put(txn1, 30, "30")
	txn1.Commit()

	go func() {
		txn2 := engine.NewTxn()
		_ = engine.Put(txn2, 30, "40")
		txn2.Commit()
	}()

	txn3 := engine.NewTxn()
	val, _ := engine.Get(txn3, 30)
	if val != "30" {
		t.Errorf("Expect 30, got %v\n", val)
	}
	txn3.Commit()
}

func Test_Deadlock(t *testing.T) {
	engine := NewUint64Engine().Run()

	done := sync.WaitGroup{}
	done.Add(2)

	latch := sync.WaitGroup{}
	latch.Add(2)
	go func() {
		txn1 := engine.NewTxn()

		_ = engine.Put(txn1, 30, "31")
		latch.Done()
		latch.Wait()
		err := engine.Put(txn1, 40, "41")
		if err != nil {
			t.Logf("txn1 aborted: %v", err)
		}

		txn1.Commit()
		done.Done()
	}()

	go func() {
		txn2 := engine.NewTxn()

		_ = engine.Put(txn2, 40, "42")
		latch.Done()
		latch.Wait()
		err := engine.Put(txn2, 30, "32")
		if err != nil {
			t.Logf("txn2 aborted: %v", err)
		}

		txn2.Commit()
		done.Done()
	}()

	done.Wait()
}

func Test_Vacuum(t *testing.T) {
	const scale = 1000
	engine := NewUint64Engine().Run()

	for i := 0; i < scale; i++ {
		txn := engine.NewTxn()
		err := engine.Put(txn, 1, strconv.Itoa(i))
		if err != nil {
			t.Error(t)
		}
		txn.Commit()
	}

	time.Sleep(time.Second)
	txn := engine.NewTxn()
	version, err := engine.GetVersion(txn, 1)
	if err != nil {
		t.Error(t)
	}

	count := 0
	for version != nil {
		version = version.Next
		count++
	}
	t.Logf("count: %d", count)
	if count != 1 {
		t.Errorf("Expect 1, got %d\n", count)
	}
	txn.Commit()
}

func Test_Concurrency(t *testing.T) {
	const scale = 250000
	engine := NewUint64Engine().Run()

	done := sync.WaitGroup{}
	done.Add(scale - 1)
	for i := 1; i < scale; i++ {
		go func(i uint64) {
			txn := engine.NewTxn()
			defer txn.Commit()

			err := engine.Put(txn, i, strconv.Itoa(int(i)))
			if err != nil {
				t.Error(err)
			}
			done.Done()
		}(uint64(i))
	}

	done.Wait()
	txn := engine.NewTxn()
	defer txn.Commit()
	for i := 1; i < scale; i++ {
		val, err := engine.Get(txn, uint64(i))
		if err != nil {
			t.Error(err)
		} else if val != strconv.Itoa(i) {
			t.Fatalf("Expect %d, got %s", i, val)
		}
	}
}

func Test_StringEngine_Concurrency(t *testing.T) {
	const scale = 150000
	engine := NewStringEngine().Run()

	done := sync.WaitGroup{}
	done.Add(scale - 1)
	for i := 1; i < scale; i++ {
		go func(i int) {
			txn := engine.NewTxn()
			defer txn.Commit()

			key := strconv.Itoa(i)
			err := engine.Put(txn, key, key)
			if err != nil {
				t.Error(err)
			}
			done.Done()
		}(i)
	}

	done.Wait()
	txn := engine.NewTxn()
	defer txn.Commit()
	for i := 1; i < scale; i++ {
		key := strconv.Itoa(i)
		val, err := engine.Get(txn, key)
		if err != nil {
			t.Error(err)
		} else if val != key {
			t.Fatalf("Expect %d, got %s", i, val)
		}
	}
}

func Test_DirtyWrite(t *testing.T) {
	engine := NewStringEngine().Run()

	done := &sync.WaitGroup{}
	done.Add(2)

	t1 := NewThread(done).Run()
	t2 := NewThread(done).Run()

	txn1 := engine.NewTxn()
	txn2 := engine.NewTxn()
	t1.Do(func() bool {
		err := engine.Put(txn1, "A", "1")
		if err != nil {
			t.Error(err)
		}
		return false
	})

	time.Sleep(time.Millisecond)
	t2.Do(func() bool {
		err := engine.Put(txn2, "A", "2")
		if err != nil {
			t.Error(err)
		}
		txn2.Commit()
		return true
	})

	t1.Do(func() bool {
		txn1.Abort()
		return true
	})
	done.Wait()

	txn := engine.NewTxn()
	defer txn.Commit()
	val, err := engine.Get(txn, "A")
	if err != nil {
		t.Error(err)
	}

	if val != "2" {
		t.Logf("Expect %s, got %s\n", "2", val)
	}
}

func Test_DirtyRead(t *testing.T) {
	engine := NewStringEngine().Run()

	done := &sync.WaitGroup{}
	done.Add(2)

	t1 := NewThread(done).Run()
	t2 := NewThread(done).Run()

	txn1 := engine.NewTxn()
	txn2 := engine.NewTxn()
	t1.Do(func() bool {
		err := engine.Put(txn1, "A", "1")
		if err != nil {
			t.Error(err)
		}
		return false
	})

	t2.Do(func() bool {
		val, err := engine.Get(txn2, "A")
		if err == nil {
			t.Fatalf("Expect err, but got val=%v", val)
		}

		txn2.Commit()
		return true
	})

	t1.Do(func() bool {
		txn1.Abort()
		return true
	})
	done.Wait()
}

func Test_LostUpdate(t *testing.T) {
	engine := NewStringEngine().Run()

	txn := engine.NewTxn()
	_ = engine.Put(txn, "A", "Null")
	txn.Commit()

	done := &sync.WaitGroup{}
	done.Add(2)

	t1 := NewThread(done).Run()
	t2 := NewThread(done).Run()

	txn1 := engine.NewTxn()
	txn2 := engine.NewTxn()
	var tmp string
	t1.Do(func() bool {
		var err error
		tmp, err = engine.Get(txn1, "A")
		if err != nil {
			t.Error(err)
		}
		return false
	})

	t2.Do(func() bool {
		val, err := engine.Get(txn2, "A")
		if err != nil {
			t.Error(err)
		}

		err = engine.Put(txn2, "A", val+":t2")
		if err != nil {
			t.Log(err)
		}

		txn2.Commit()
		return true
	})

	t1.Do(func() bool {
		err := engine.Put(txn1, "A", tmp+":t1")
		if err != nil {
			t.Log(err)
		}

		txn1.Commit()
		return true
	})
	done.Wait()

	txn = engine.NewTxn()
	defer txn.Commit()
	val, err := engine.Get(txn, "A")
	if err != nil {
		t.Error(err)
	}

	// one of two txns should be aborted
	expect := []string{"Null:t2", "Null:t1"}
	if val != expect[0] && val != expect[1] {
		t.Fatalf("Expect %v, got %s\n", expect, val)
	}
}

func Test_NonrepeatableRead(t *testing.T) {
	engine := NewStringEngine().Run()

	txn := engine.NewTxn()
	_ = engine.Put(txn, "A", "Null")
	txn.Commit()

	done := &sync.WaitGroup{}
	done.Add(2)

	t1 := NewThread(done).Run()
	t2 := NewThread(done).Run()

	txn1 := engine.NewTxn()
	txn2 := engine.NewTxn()
	t1.Do(func() bool {
		val, err := engine.Get(txn1, "A")
		if err != nil {
			t.Error(err)
		}
		if val != "Null" {
			t.Fatalf("Expect %v, got %s\n", "Null", val)
		}
		return false
	})

	t2.Do(func() bool {
		err := engine.Put(txn2, "A", "txn2")
		if err != nil {
			t.Error(err)
		}
		txn2.Commit()
		return true
	})

	t1.Do(func() bool {
		val, err := engine.Get(txn1, "A")
		if err != nil {
			t.Error(err)
		}
		if val != "Null" {
			t.Fatalf("Expect %v, got %s\n", "Null", val)
		}
		txn1.Commit()
		return true
	})
	done.Wait()

	txn = engine.NewTxn()
	defer txn.Commit()
	val, _ := engine.Get(txn, "A")
	if val != "txn2" {
		t.Fatalf("Expect txn2, got %s\n", val)
	}
}

func Test_ReadSkew(t *testing.T) {
	engine := NewStringEngine().Run()

	txn := engine.NewTxn()
	_ = engine.Put(txn, "A", "5")
	_ = engine.Put(txn, "B", "5")
	txn.Commit()

	done := &sync.WaitGroup{}
	done.Add(2)

	t1 := NewThread(done).Run()
	t2 := NewThread(done).Run()

	txn1 := engine.NewTxn()
	txn2 := engine.NewTxn()
	t1.Do(func() bool {
		val, err := engine.Get(txn1, "A")
		if err != nil {
			t.Error(err)
		}
		if val != "5" {
			t.Fatalf("Expect %v, got %s\n", "5", val)
		}
		return false
	})

	t2.Do(func() bool {
		err := engine.Put(txn2, "A", "0")
		if err != nil {
			t.Error(err)
		}
		err = engine.Put(txn2, "B", "10")
		if err != nil {
			t.Error(err)
		}
		txn2.Commit()
		return true
	})

	t1.Do(func() bool {
		val, err := engine.Get(txn1, "B")
		if err != nil {
			t.Error(err)
		}
		if val != "5" {
			t.Fatalf("Expect %v, got %s\n", "5", val)
		}
		txn1.Commit()
		return true
	})
	done.Wait()

	txn = engine.NewTxn()
	defer txn.Commit()
	A, _ := engine.Get(txn, "A")
	if A != "0" {
		t.Fatalf("Expect 0, got %s\n", A)
	}

	B, _ := engine.Get(txn, "B")
	if B != "10" {
		t.Fatalf("Expect 10, got %s\n", B)
	}
}
