package mquery

import (
	"context"
	"fmt"
	"strings"

	jsoniter "github.com/json-iterator/go"
	"github.com/moremorefun/mcommon"
)

// FormatMapKey 格式化字段名到key
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
func DbGetValuesFromMapRows(ms map[string][]map[string]interface{}, key string) ([]interface{}, error) {
	key = FormatMapKey(key)
	var values []interface{}
	for _, rows := range ms {
		for _, row := range rows {
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

// DbSelectRows2One 获取关联map
func DbSelectRows2One(ctx context.Context, tx mcommon.DbExeAble, sourceRows []map[string]interface{}, sourceKey, targetTableName, targetKey string, targetColumns []string) (map[string]map[string]interface{}, []interface{}, error) {
	keyValues, err := DbGetValuesFromRows(sourceRows, sourceKey)
	if err != nil {
		return nil, nil, err
	}
	targetMap, err := DbSelectKeys2One(ctx, tx, keyValues, targetTableName, targetKey, targetColumns)
	return targetMap, keyValues, err
}

// DbSelectRows2Many 获取关联map
func DbSelectRows2Many(ctx context.Context, tx mcommon.DbExeAble, sourceRows []map[string]interface{}, sourceKey, targetTableName, targetKey string, targetColumns []string) (map[string][]map[string]interface{}, []interface{}, error) {
	keyValues, err := DbGetValuesFromRows(sourceRows, sourceKey)
	if err != nil {
		return nil, nil, err
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
		Rows(
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
		Rows(
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

// DbInterfaceToStruct 转换到struct
func DbInterfaceToStruct(inc interface{}, s interface{}) error {
	b, err := jsoniter.Marshal(inc)
	if err != nil {
		return err
	}
	err = jsoniter.Unmarshal(b, s)
	if err != nil {
		return err
	}
	return nil
}
