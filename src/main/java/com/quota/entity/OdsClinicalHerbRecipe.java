package com.quota.entity;

import java.util.Date;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OdsClinicalHerbRecipe {

	private String cevent;

	private java.math.BigDecimal recipeId;//

	private java.math.BigDecimal cliOrderId;//

	private Integer seqNo;//

	private String recipeNo;//

	private java.math.BigDecimal recipeStatusFlag;//

	private java.math.BigDecimal recipeAmount;//

	private java.math.BigDecimal pharmacyId;//

	private java.math.BigDecimal encounterId;//

	private java.math.BigDecimal printedFlag;//

	private java.math.BigDecimal printerId;//

	private Date printedAt;//

	private java.math.BigDecimal hospitalSoid;//

	private Short isDel;//

	private Date createdAt;//

	private Date modifiedAt;//

	private Date odsCreatedate;//

	private String odsId;//

	private Short odsState;//

	private Date odsTimestamp;//

	private String id;//

	private Long ts;

	public Long getTs() {
		return new Date().getTime();
	}

}
