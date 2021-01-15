package mquery

import (
	"bytes"
	"strings"
)

// getK 获取key
func getK(old string) string {
	old = strings.ReplaceAll(old, ".", "_")
	old = strings.ReplaceAll(old, "`", "_")
	return old
}

// ConvertRaw 原样生成
type ConvertRaw string

func (o ConvertRaw) AppendToQuery(buf bytes.Buffer, arg map[string]interface{}) (bytes.Buffer, map[string]interface{}, error) {
	_, err := buf.WriteString(string(o))
	if err != nil {
		return bytes.Buffer{}, nil, err
	}
	return buf, arg, nil
}

// ConvertKv kv结构
type ConvertKv struct {
	K string
	V interface{}
}

// ConvertKvStr kv字符串
type ConvertKvStr struct {
	K string
	V string
}

// ConvertEq k=:k
type ConvertEq ConvertKv

func (o ConvertEq) AppendToQuery(buf bytes.Buffer, arg map[string]interface{}) (bytes.Buffer, map[string]interface{}, error) {
	k := getK(o.K)

	_, err := buf.WriteString(o.K)
	if err != nil {
		return bytes.Buffer{}, nil, err
	}
	_, err = buf.WriteString("=:")
	if err != nil {
		return bytes.Buffer{}, nil, err
	}
	_, err = buf.WriteString(k)
	if err != nil {
		return bytes.Buffer{}, nil, err
	}
	arg[k] = o.V
	return buf, arg, nil
}

// ConvertGt k>:k
type ConvertGt ConvertKv

func (o ConvertGt) AppendToQuery(buf bytes.Buffer, arg map[string]interface{}) (bytes.Buffer, map[string]interface{}, error) {
	k := getK(o.K)

	_, err := buf.WriteString(o.K)
	if err != nil {
		return bytes.Buffer{}, nil, err
	}
	_, err = buf.WriteString(">:")
	if err != nil {
		return bytes.Buffer{}, nil, err
	}
	_, err = buf.WriteString(k)
	if err != nil {
		return bytes.Buffer{}, nil, err
	}
	arg[k] = o.V
	return buf, arg, nil
}

// ConvertLt k<:k
type ConvertLt ConvertKv

func (o ConvertLt) AppendToQuery(buf bytes.Buffer, arg map[string]interface{}) (bytes.Buffer, map[string]interface{}, error) {
	k := getK(o.K)

	_, err := buf.WriteString(o.K)
	if err != nil {
		return bytes.Buffer{}, nil, err
	}
	_, err = buf.WriteString("<:")
	if err != nil {
		return bytes.Buffer{}, nil, err
	}
	_, err = buf.WriteString(k)
	if err != nil {
		return bytes.Buffer{}, nil, err
	}
	arg[k] = o.V
	return buf, arg, nil
}

// ConvertEqRaw k=v
type ConvertEqRaw ConvertKvStr

func (o ConvertEqRaw) AppendToQuery(buf bytes.Buffer, arg map[string]interface{}) (bytes.Buffer, map[string]interface{}, error) {
	_, err := buf.WriteString(o.K)
	if err != nil {
		return bytes.Buffer{}, nil, err
	}
	_, err = buf.WriteString("=")
	if err != nil {
		return bytes.Buffer{}, nil, err
	}
	_, err = buf.WriteString(o.V)
	if err != nil {
		return bytes.Buffer{}, nil, err
	}
	return buf, arg, nil
}

// ConvertDesc k DESC
type ConvertDesc string

func (o ConvertDesc) AppendToQuery(buf bytes.Buffer, arg map[string]interface{}) (bytes.Buffer, map[string]interface{}, error) {
	_, err := buf.WriteString(string(o))
	if err != nil {
		return bytes.Buffer{}, nil, err
	}
	_, err = buf.WriteString(" DESC")
	if err != nil {
		return bytes.Buffer{}, nil, err
	}
	return buf, arg, nil
}

// ConvertValue k=VALUE(k)
type ConvertValue string

func (o ConvertValue) AppendToQuery(buf bytes.Buffer, arg map[string]interface{}) (bytes.Buffer, map[string]interface{}, error) {
	_, err := buf.WriteString(string(o))
	if err != nil {
		return bytes.Buffer{}, nil, err
	}
	_, err = buf.WriteString("=VALUE(")
	if err != nil {
		return bytes.Buffer{}, nil, err
	}
	_, err = buf.WriteString(string(o))
	if err != nil {
		return bytes.Buffer{}, nil, err
	}
	_, err = buf.WriteString(")")
	if err != nil {
		return bytes.Buffer{}, nil, err
	}
	return buf, arg, nil
}
