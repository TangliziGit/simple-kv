package parsers

import (
	"fmt"
	"simple-kv/pkg/protos"
	"unicode"
)

type Parser struct {
	Input  string
	Length int
}

func NewParser() *Parser {
	return &Parser{}
}

/*
<command>  := <type> <strings>
<strings>  := <string> <strings>
			| <string>
<string>   := " .*? "
*/

func (p *Parser) Parse(input string) (*protos.Command, error) {
	p.Input = input
	p.Length = len(input)

	t, next, err := p.getType(0)
	if err != nil {
		return nil, err
	}

	var content []string
	switch t {
	case protos.Get, protos.Del:
		next, err = p.dropSpaces(next)
		if err != nil {
			return nil, err
		}

		var key string
		key, next, err = p.getString(next)
		content = append(content, key)

	case protos.Put:
		next, err = p.dropSpaces(next)
		if err != nil {
			return nil, err
		}

		var key, val string
		if key, next, err = p.getString(next); err != nil {
			break
		}
		if next, err = p.dropSpaces(next); err != nil {
			break
		}
		if val, next, err = p.getString(next); err != nil {
			break
		}
		content = append(content, key, val)

	case protos.Scan:
		next, err = p.dropSpaces(next)
		if err != nil {
			return nil, err
		}

		var key, count string
		if key, next, err = p.getString(next); err != nil {
			break
		}
		if next, err = p.dropSpaces(next); err != nil {
			break
		}
		if count, next, err = p.getChars(next, unicode.IsDigit); err != nil {
			break
		}
		content = append(content, key, count)

	case protos.Begin, protos.Commit, protos.Abort:
		err = nil
	default:
		err = fmt.Errorf("invalid type:\n%s", p.errorOn(next-1))
	}

	if err != nil {
		return nil, err
	}

	if next != p.Length {
		next, _ = p.dropSpaces(next)
	}

	if next != p.Length {
		return nil, fmt.Errorf("command should terminated here:\n%s", p.errorOn(next))
	}

	return protos.NewCommand(t, content), nil
}

func (p *Parser) dropSpaces(i int) (int, error) {
	if i >= p.Length {
		return i, fmt.Errorf("should not be terminiated here:\n%s", p.errorOn(i))
	}

	if p.Input[i] != ' ' {
		return i, fmt.Errorf("a white space needed here:\n%s", p.errorOn(i))
	}

	for i++; i < p.Length; i++ {
		if p.Input[i] != ' ' {
			break
		}
	}

	return i, nil
}

func (p *Parser) getString(i int) (string, int, error) {
	if i >= p.Length {
		return "", i, fmt.Errorf("should not be terminiated here:\n%s", p.errorOn(i))
	}

	if p.Input[i] != '"' {
		return "", i, fmt.Errorf("a quotation mark needed here:\n%s", p.errorOn(i))
	}

	j := i + 1
	for ; j < p.Length; j++ {
		if p.Input[j] == '"' {
			break
		}
	}

	if j >= p.Length || p.Input[j] != '"' {
		return "", j, fmt.Errorf("a quotation mark needed here:\n%s", p.errorOn(j))
	}

	return p.Input[i+1 : j], j + 1, nil
}

func (p *Parser) getType(i int) (protos.CommandType, int, error) {
	if i >= p.Length {
		return protos.Invalid, i, fmt.Errorf("should not be terminiated here:\n%s", p.errorOn(i))
	}

	t, next, err := p.getChars(i, unicode.IsLetter)
	if err != nil {
		return protos.Invalid, i, err
	}

	return protos.ToCommandType(t), next, err
}

func (p *Parser) getChars(i int, check func(rune) bool) (string, int, error) {
	if i >= p.Length {
		return "", i, fmt.Errorf("should not be terminiated here:\n%s", p.errorOn(i))
	}

	j := i
	for ; j < p.Length; j++ {
		if !check(rune(p.Input[j])) {
			break
		}
	}
	return p.Input[i:j], j, nil
}

func (p *Parser) errorOn(idx int) string {
	var pad string
	for i := 0; i < idx; i++ {
		pad += " "
	}
	return p.Input + pad + "^"
}
