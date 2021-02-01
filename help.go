package mquery

import (
	"context"
	"fmt"
	"strings"

	"github.com/moremorefun/mcommon"
)

func FormatMapKey(oldKey string) string {
	oldKey = strings.ReplaceAll(oldKey, "`", "")
	lastIndex := strings.LastIndex(oldKey, ".")
	if lastIndex != -1 {
		oldKey = oldKey[lastIndex+1:]
	}
	return oldKey
}

// DbGetValuesFromRows 获取values
func DbGetValuesFromRows(rows []map[string]interface{}, key string) ([]interface{}, error) {
	key = FormatMapKey(key)
	var values []interface{}
	for _, row := range rows {
		v, ok := row[key]
		if !ok {
			return nil, fmt.Errorf("no key: %s", key)
		}
		if !mcommon.IsInSlice(values, v) {
			values = append(values, v)
		}
	}
	return values, nil
}

// DbGetValuesFromMap 获取values
func DbGetValuesFromMap(m map[string]map[string]interface{}, key string) ([]interface{}, error) {
	key = FormatMapKey(key)
	var values []interface{}
	for _, row := range m {
		v, ok := row[key]
		if !ok {
			return nil, fmt.Errorf("no key: %s", key)
		}
		if !mcommon.IsInSlice(values, v) {
			values = append(values, v)
		}
	}
	return values, nil
}

// DbGetValuesFromMapRows 获取values
func DbGetValuesFromMapRows(ms []map[string]map[string]interface{}, key string) ([]interface{}, error) {
	key = FormatMapKey(key)
	var values []interface{}
	for _, m := range ms {
		for _, row := range m {
			v, ok := row[key]
			if !ok {
				return nil, fmt.Errorf("no key: %s", key)
			}
			if !mcommon.IsInSlice(values, v) {
				values = append(values, v)
			}
		}
	}
	return values, nil
}

// DbSelectMapOne2One 获取关联map
func DbSelectMapOne2One(ctx context.Context, tx mcommon.DbExeAble, sourceRows []map[string]interface{}, sourceKey, targetTableName, targetKey string, targetColumns []string) (map[string]map[string]interface{}, []interface{}, error) {
	var keyValues []interface{}
	sourceKey = FormatMapKey(sourceKey)
	for _, sourceRow := range sourceRows {
		v, ok := sourceRow[sourceKey]
		if !ok {
			return nil, nil, fmt.Errorf("no source key: %s", sourceKey)
		}
		if !mcommon.IsInSlice(keyValues, v) {
			keyValues = append(keyValues, v)
		}
	}
	targetMap, err := DbSelectKeys2One(ctx, tx, keyValues, targetTableName, targetKey, targetColumns)
	return targetMap, keyValues, err
}

// DbSelectMapOne2Many 获取关联map
func DbSelectMapOne2Many(ctx context.Context, tx mcommon.DbExeAble, sourceRows []map[string]interface{}, sourceKey, targetTableName, targetKey string, targetColumns []string) (map[string][]map[string]interface{}, []interface{}, error) {
	var keyValues []interface{}
	sourceKey = FormatMapKey(sourceKey)
	for _, sourceRow := range sourceRows {
		v, ok := sourceRow[sourceKey]
		if !ok {
			return nil, nil, fmt.Errorf("no source key: %s", sourceKey)
		}
		if !mcommon.IsInSlice(keyValues, v) {
			keyValues = append(keyValues, v)
		}
	}
	targetMap, err := DbSelectKeys2Many(ctx, tx, keyValues, targetTableName, targetKey, targetColumns)
	return targetMap, keyValues, err
}

// DbSelectKeys2One 获取关联map
func DbSelectKeys2One(ctx context.Context, tx mcommon.DbExeAble, keyValues []interface{}, targetTableName, targetKey string, targetColumns []string) (map[string]map[string]interface{}, error) {
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
	mapTargetKey := FormatMapKey(targetKey)
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

// DbSelectKeys2Many 获取关联map
func DbSelectKeys2Many(ctx context.Context, tx mcommon.DbExeAble, keyValues []interface{}, targetTableName, targetKey string, targetColumns []string) (map[string][]map[string]interface{}, error) {
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
	mapTargetKey := FormatMapKey(targetKey)
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
