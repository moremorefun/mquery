package mquery

import (
	"context"
	"fmt"
	"strings"

	"github.com/moremorefun/mcommon"
)

func formatMapKey(oldKey string) string {
	oldKey = strings.ReplaceAll(oldKey, "`", "")
	lastIndex := strings.LastIndex(oldKey, ".")
	if lastIndex != -1 {
		oldKey = oldKey[lastIndex+1:]
	}
	return oldKey
}

// DbSelectMapOne2One 获取关联map
func DbSelectMapOne2One(ctx context.Context, tx mcommon.DbExeAble, sourceRows []map[string]interface{}, sourceKey, targetTableName, targetKey string, targetColumns []string) (map[string]map[string]interface{}, error) {
	var keyValues []interface{}
	sourceKey = formatMapKey(sourceKey)
	for _, sourceRow := range sourceRows {
		v, ok := sourceRow[sourceKey]
		if !ok {
			return nil, fmt.Errorf("no source key: %s", sourceKey)
		}
		if !mcommon.IsInSlice(keyValues, v) {
			keyValues = append(keyValues, v)
		}
	}
	if len(keyValues) == 0 {
		return nil, nil
	}
	if len(targetColumns) != 0 {
		if !mcommon.IsStringInSlice(targetColumns, targetKey) {
			targetColumns = append(targetColumns, targetKey)
		}
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
	mapTargetKey := formatMapKey(targetKey)
	targetMap := map[string]map[string]interface{}{}
	for _, targetRow := range targetRows {
		kv, ok := targetRow[mapTargetKey]
		if !ok {
			return nil, fmt.Errorf("no target key: %s", mapTargetKey)
		}
		k := fmt.Sprintf("%v", kv)
		targetMap[k] = targetRow
	}
	return targetMap, nil
}

// DbSelectMapOne2Many 获取关联map
func DbSelectMapOne2Many(ctx context.Context, tx mcommon.DbExeAble, sourceRows []map[string]interface{}, sourceKey, targetTableName, targetKey string, targetColumns []string) (map[string][]map[string]interface{}, error) {
	var keyValues []interface{}
	sourceKey = formatMapKey(sourceKey)
	for _, sourceRow := range sourceRows {
		v, ok := sourceRow[sourceKey]
		if !ok {
			return nil, fmt.Errorf("no source key: %s", sourceKey)
		}
		if !mcommon.IsInSlice(keyValues, v) {
			keyValues = append(keyValues, v)
		}
	}
	if len(keyValues) == 0 {
		return nil, nil
	}
	if len(targetColumns) != 0 {
		if !mcommon.IsStringInSlice(targetColumns, targetKey) {
			targetColumns = append(targetColumns, targetKey)
		}
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
	mapTargetKey := formatMapKey(targetKey)
	targetMap := map[string][]map[string]interface{}{}
	for _, targetRow := range targetRows {
		kv, ok := targetRow[mapTargetKey]
		if !ok {
			return nil, fmt.Errorf("no target key: %s", mapTargetKey)
		}
		k := fmt.Sprintf("%v", kv)
		targetMap[k] = append(targetMap[k], targetRow)
	}
	return targetMap, nil
}
