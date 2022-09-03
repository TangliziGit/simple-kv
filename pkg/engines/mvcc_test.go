package engines

import (
	"strconv"
	"sync"
	"testing"
	"time"
)

func Test1(t *testing.T) {
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

func Test2(t *testing.T) {
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

func Test3_Deadlock(t *testing.T) {
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

func Test4_Vacuum(t *testing.T) {
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

func Test5_Concurrency(t *testing.T) {
	const scale = 200000
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
			t.Logf("Expect %d, got %s", i, val)
		}
	}
}

func Test6_StringEngine_Concurrency(t *testing.T) {
	const scale = 100000
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
			t.Logf("Expect %d, got %s", i, val)
		}
	}
}
