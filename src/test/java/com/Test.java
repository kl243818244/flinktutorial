package com;

public class Test {
	
	public static void main(String[] args) {
		
		String sql = "select rownum , category_id , cnt from (\r\n" + 
				"	select category_id , cnt ,  ROW_NUMBER() OVER ( ORDER BY cnt DESC  ) AS rownum from ( select category_id , count(category_id) as cnt from user_behavior group by category_id )\r\n" + 
				")\r\n" + 
				"where rownum <= 2";
		
		
		
		System.out.println(sql);
		
		
	}
	

}
