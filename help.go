package mquery

import (
	"context"
	"fmt"

	"github.com/moremorefun/mcommon"
)

// DbSelectMapOne2One 获取关联map
func DbSelectMapOne2One(ctx context.Context, tx mcommon.DbExeAble, sourceRows []map[string]interface{}, sourceKey, targetTableName, targetKey string, targetColumns []string) (map[string]interface{}, error) {
	var keyValues []interface{}
	for _, sourceRow := range sourceRows {
		keyValues = append(keyValues, sourceRow[sourceKey])
	}
	if len(keyValues) == 0 {
		return nil, nil
	}
	targetRows, err := Select().
		ColumnsString(targetColumns...).
		FromString(targetTableName).
		Where(ConvertEqMake(targetKey, keyValues)).
		RowsInterface(
			ctx,
			tx,
		)
	if err != nil {
		return nil, err
	}
	targetMap := map[string]interface{}{}
	for _, targetRow := range targetRows {
		k := fmt.Sprintf("%v", targetRow[targetKey])
		targetMap[k] = targetRow
	}
	return targetMap, nil
}

// DbSelectMapOne2Many 获取关联map
func DbSelectMapOne2Many(ctx context.Context, tx mcommon.DbExeAble, sourceRows []map[string]interface{}, sourceKey, targetTableName, targetKey string, targetColumns []string) (map[string][]interface{}, error) {
	var keyValues []interface{}
	for _, sourceRow := range sourceRows {
		keyValues = append(keyValues, sourceRow[sourceKey])
	}
	if len(keyValues) == 0 {
		return nil, nil
	}
	targetRows, err := Select().
		ColumnsString(targetColumns...).
		FromString(targetTableName).
		Where(ConvertEqMake(targetKey, keyValues)).
		RowsInterface(
			ctx,
			tx,
		)
	if err != nil {
		return nil, err
	}
	targetMap := map[string][]interface{}{}
	for _, targetRow := range targetRows {
		k := fmt.Sprintf("%v", targetRow[targetKey])
		targetMap[k] = append(targetMap[k], targetRow)
	}
	return targetMap, nil
}
