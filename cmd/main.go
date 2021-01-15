package main

import (
	"fmt"

	"github.com/moremorefun/mquery"
)

func main() {
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
