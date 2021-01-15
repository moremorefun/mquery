package main

import (
	"fmt"

	"github.com/moremorefun/mquery"
)

func main() {
	//tSelect()
	//tInsert()
	//tUpdate()
	tDelete()
}

func tSelect() {
	query, arg, err := mquery.
		Select().
		ColumnsString("id", "user_name").
		FromString("t_user").
		Join(
			mquery.
				Join(mquery.JoinTypeInner).
				TableString("t_user_coin").
				On(
					mquery.ConvertEqRaw{
						K: "t_user_coin.user_id",
						V: "t_user.id",
					},
					mquery.ConvertEq{
						K: "t_user_coin.coin",
						V: 12,
					},
				),
		).
		Where(
			mquery.ConvertEq{
				K: "t_user.id",
				V: 1,
			},
		).
		GroupBysString("t_user.id").
		OrderBysString(
			"t_user.id",
		).
		Offset(1).
		Limit(100).
		ForUpdate().
		ToSQL()
	if err != nil {
		fmt.Printf("%s\n", err.Error())
		return
	}
	fmt.Printf("%s\n%#v\n", query, arg)
}

func tInsert() {
	query, arg, err := mquery.
		Insert().
		Ignore().
		Into("t_user").
		Columns(
			"id",
			"user_name",
		).
		Values(
			[]interface{}{
				1,
				"hao",
			},
			[]interface{}{
				2,
				"hao1",
			},
		).
		Duplicates(
			mquery.ConvertValue("user_name"),
			mquery.ConvertEq{
				K: "user_city",
				V: "hao",
			},
		).
		ToSQL()
	if err != nil {
		fmt.Printf("%s\n", err.Error())
		return
	}
	fmt.Printf("%s\n%#v\n", query, arg)
}

func tUpdate() {
	query, arg, err := mquery.
		Update().
		Table("t_user").
		Update(
			mquery.ConvertEq{
				K: "t_user.name",
				V: "hao",
			},
		).
		Where(
			mquery.ConvertEq{
				K: "t_user.id",
				V: 1,
			},
		).
		ToSQL()
	if err != nil {
		fmt.Printf("%s\n", err.Error())
		return
	}
	fmt.Printf("%s\n%#v\n", query, arg)
}

func tDelete() {
	query, arg, err := mquery.
		Delete().
		Table("t_user").
		Where(
			mquery.ConvertEq{
				K: "t_user.id",
				V: 1,
			},
			mquery.ConvertEq{
				K: "t_user.id",
				V: 1,
			},
		).
		ToSQL()
	if err != nil {
		fmt.Printf("%s\n", err.Error())
		return
	}
	fmt.Printf("%s\n%#v\n", query, arg)
}
