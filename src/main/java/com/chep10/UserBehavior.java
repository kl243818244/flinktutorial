package com.chep10;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserBehavior {

	private long userId; // 用户ID
	private long itemId; // 商品ID
	private int categoryId; // 商品类目ID
	private String behavior; // 用户行为, 包括("pv", "buy", "cart", "fav")
	private long timestamp; // 行为发生的时间戳，单位秒

}
