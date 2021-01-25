package mquery

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

var globalIndex int64

// getK 获取key
func getK(old string) string {
	globalIndex++
	old = strings.ReplaceAll(old, ".", "_")
	old = strings.ReplaceAll(old, "`", "_")
	var buf bytes.Buffer
	buf.WriteString(old)
	buf.WriteString("_")
	buf.WriteString(strconv.FormatInt(globalIndex, 10))
	return buf.String()
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

// ConvertEqMake 生成
func ConvertEqMake(k string, v interface{}) ConvertEq {
	return ConvertEq{
		K: k,
		V: v,
	}
}

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

// ConvertAdd k=k+:k
type ConvertAdd ConvertKv

// ConvertAddMake 生成
func ConvertAddMake(k string, v interface{}) ConvertAdd {
	return ConvertAdd{
		K: k,
		V: v,
	}
}

func (o ConvertAdd) AppendToQuery(buf bytes.Buffer, arg map[string]interface{}) (bytes.Buffer, map[string]interface{}, error) {
	k := getK(o.K)

	_, err := buf.WriteString(o.K)
	if err != nil {
		return bytes.Buffer{}, nil, err
	}
	_, err = buf.WriteString("=")
	if err != nil {
		return bytes.Buffer{}, nil, err
	}
	_, err = buf.WriteString(o.K)
	if err != nil {
		return bytes.Buffer{}, nil, err
	}
	_, err = buf.WriteString("+:")
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

// ConvertGtMake 生成
func ConvertGtMake(k string, v interface{}) ConvertGt {
	return ConvertGt{
		K: k,
		V: v,
	}
}

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

// ConvertLtMake 生成
func ConvertLtMake(k string, v interface{}) ConvertLt {
	return ConvertLt{
		K: k,
		V: v,
	}
}

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

// ConvertEqRawMake 生成
func ConvertEqRawMake(k, v string) ConvertEqRaw {
	return ConvertEqRaw{
		K: k,
		V: v,
	}
}

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

type ConvertOr struct {
	Left  SQLAble
	Right SQLAble
}

// ConvertOrMake 生成
func ConvertOrMake(left, right SQLAble) ConvertOr {
	return ConvertOr{
		Left:  left,
		Right: right,
	}
}

func (o ConvertOr) AppendToQuery(buf bytes.Buffer, arg map[string]interface{}) (bytes.Buffer, map[string]interface{}, error) {
	var err error
	if o.Left == nil || o.Right == nil {
		return bytes.Buffer{}, nil, fmt.Errorf("or empty")
	}
	buf.WriteString("(")
	buf, arg, err = o.Left.AppendToQuery(buf, arg)
	if err != nil {
		return bytes.Buffer{}, nil, err
	}
	buf.WriteString(" or ")
	buf, arg, err = o.Right.AppendToQuery(buf, arg)
	if err != nil {
		return bytes.Buffer{}, nil, err
	}
	buf.WriteString(" )")
	return buf, arg, nil
}
