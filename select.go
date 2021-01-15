package mquery

import (
	"bytes"
	"fmt"
	"strconv"
)

type selectData struct {
	columns      []SQLAble
	from         SQLAble
	joins        []SQLAble
	whereParts   []SQLAble
	groupBys     []SQLAble
	orderByParts []SQLAble
	offset       int64
	limit        int64
	isForUpdate  bool
}

// Select 创建搜索
func Select() *selectData {
	var q selectData
	return &q
}

// Columns 字段
func (q *selectData) Columns(columns ...SQLAble) *selectData {
	q.columns = append(q.columns, columns...)
	return q
}

// ColumnsString 字段
func (q *selectData) ColumnsString(columns ...string) *selectData {
	for _, column := range columns {
		q.columns = append(q.columns, ConvertRaw(column))
	}
	return q
}

// From 表名
func (q *selectData) From(from SQLAble) *selectData {
	q.from = from
	return q
}

// FromString 表名
func (q *selectData) FromString(from string) *selectData {
	q.from = ConvertRaw(from)
	return q
}

// Where 条件
func (q *selectData) Where(cond ...SQLAble) *selectData {
	q.whereParts = append(q.whereParts, cond...)
	return q
}

// GroupBys 分组
func (q *selectData) GroupBys(groupBys ...SQLAble) *selectData {
	q.groupBys = append(q.groupBys, groupBys...)
	return q
}

// GroupBysString 分组
func (q *selectData) GroupBysString(groupBys ...string) *selectData {
	for _, groupBy := range groupBys {
		q.groupBys = append(q.groupBys, ConvertRaw(groupBy))
	}
	return q
}

// OrderBys 排序
func (q *selectData) OrderBys(orders ...SQLAble) *selectData {
	q.orderByParts = append(q.orderByParts, orders...)
	return q
}

// OrderBysString 排序
func (q *selectData) OrderBysString(orders ...string) *selectData {
	for _, order := range orders {
		q.orderByParts = append(q.orderByParts, ConvertRaw(order))
	}
	return q
}

// Limit 限制
func (q *selectData) Limit(limit int64) *selectData {
	q.limit = limit
	return q
}

// Offset 偏移
func (q *selectData) Offset(offset int64) *selectData {
	q.offset = offset
	return q
}

// QueryJoin 链接
func (q *selectData) Join(join ...SQLAble) *selectData {
	q.joins = append(q.joins, join...)
	return q
}

// ForUpdate 加锁
func (q *selectData) ForUpdate() *selectData {
	q.isForUpdate = true
	return q
}

// ToSQL 生成sql
func (q *selectData) ToSQL() (string, map[string]interface{}, error) {
	var err error
	var buf bytes.Buffer
	arg := map[string]interface{}{}

	buf.WriteString("SELECT")
	if len(q.columns) == 0 {
		buf.WriteString("\n   *")
	} else {
		lastColumnIndex := len(q.columns) - 1
		for i, column := range q.columns {
			_, err = buf.WriteString("\n    ")
			if err != nil {
				return "", nil, err
			}
			buf, arg, err = column.AppendToQuery(buf, arg)
			if err != nil {
				return "", nil, err
			}
			if i != lastColumnIndex {
				buf.WriteString(",")
			}
		}
	}

	if q.from == nil {
		return "", nil, fmt.Errorf("select no from")
	}
	buf.WriteString("\nFROM\n    ")
	buf, arg, err = q.from.AppendToQuery(buf, arg)
	if err != nil {
		return "", nil, err
	}
	if len(q.joins) > 0 {
		for _, join := range q.joins {
			buf.WriteString("\n")
			buf, arg, err = join.AppendToQuery(buf, arg)
			if err != nil {
				return "", nil, err
			}
		}
	}
	if len(q.whereParts) > 0 {
		buf.WriteString("\nWHERE")
		for i, where := range q.whereParts {
			buf.WriteString("\n    ")
			if i != 0 {
				buf.WriteString("AND ")
			}
			buf, arg, err = where.AppendToQuery(buf, arg)
			if err != nil {
				return "", nil, err
			}
		}
	}
	if len(q.groupBys) > 0 {
		buf.WriteString("\nGROUP BY\n    ")
		for i, groupBy := range q.groupBys {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf, arg, err = groupBy.AppendToQuery(buf, arg)
			if err != nil {
				return "", nil, err
			}
		}
	}
	if len(q.orderByParts) > 0 {
		buf.WriteString("\nORDER BY\n    ")
		for i, orderByPart := range q.orderByParts {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf, arg, err = orderByPart.AppendToQuery(buf, arg)
			if err != nil {
				return "", nil, err
			}
		}
	}
	if q.limit > 0 {
		if q.offset > 0 {
			buf.WriteString("\nLIMIT ")
			buf.WriteString(strconv.FormatInt(q.offset, 10))
			buf.WriteString(", ")
			buf.WriteString(strconv.FormatInt(q.limit, 10))
		} else {
			buf.WriteString("\nLIMIT ")
			buf.WriteString(strconv.FormatInt(q.limit, 10))
		}
	}
	if q.isForUpdate {
		buf.WriteString("\nFOR UPDATE")
	}
	return buf.String(), arg, nil
}
