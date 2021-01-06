package com.quota.entity;

import java.util.Date;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Settlement {
	private java.math.BigDecimal settlementId;//

	private java.math.BigDecimal chargeId;//

	private java.math.BigDecimal billId;//

	private String settlementSeqNo;//

	private java.math.BigDecimal personalIdentityId;//

	private String identityNo;//

	private java.math.BigDecimal bizRoleId;//

	private java.math.BigDecimal insuranceReimburseTypeCode;//

	private java.math.BigDecimal settlementStatus;//

	private java.math.BigDecimal settlementAccountStatus;//

	private java.math.BigDecimal settlementRetailAmount;//

	private java.math.BigDecimal settlementDiscountAmount;//

	private java.math.BigDecimal settlementInsurReimAmount;//

	private java.math.BigDecimal settlementSelfPayingAmount;//

	private java.math.BigDecimal settlementRoundingAmount;//

	private java.math.BigDecimal refundSettlementId;//

	private java.math.BigDecimal createdBy;//

	private String createEmployeeNo;//

	private java.math.BigDecimal paymentSettledBy;//

	private String paymentSettledEmployeeNo;//

	private Date settledAt;//

	private java.math.BigDecimal chrgWinId;//

	private Short isDel;//

	private Date modifiedAt;//

	private java.math.BigDecimal hospitalSoid;//

	private Date createdAt;//

	private java.math.BigDecimal posCode;//

	private java.math.BigDecimal personId;//

	private java.math.BigDecimal validFlag;//

	private java.math.BigDecimal settlementStandardAmount;//

	private String OdsId;//

	private Date OdsTimestamp;//

	private Date OdsCreatedate;//

	private Short OdsState;//

	private String Id;//
	
	private Long ts;

	public Long getTs() {
		return new Date().getTime();
	}
}
