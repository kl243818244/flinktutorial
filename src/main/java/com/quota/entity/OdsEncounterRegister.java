package com.quota.entity;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


//ODS_ENCOUNTER_REGISTER
@NoArgsConstructor
@AllArgsConstructor
@Data
public class OdsEncounterRegister implements Serializable {
	private static final long serialVersionUID = 538897534627694070L;

    private String cevent;

	private java.math.BigDecimal RegisterId;//

	private java.math.BigDecimal EncounterId;//

	private java.math.BigDecimal CsChannelTypeCode;//

	private java.math.BigDecimal EncounterChargeStatus;//


	private java.math.BigDecimal RegisteringOperatorId;//

	private java.util.Date RegisteredAt;//

	private java.math.BigDecimal CancelMethodCode;//

	private java.math.BigDecimal CancelingOperatorId;//

	private java.util.Date CanceledAt;//

	private String CancelReason;//

	private Short IsDel;//

	private java.util.Date ModifiedAt;//

	private java.util.Date CreatedAt;//

	private java.math.BigDecimal HospitalSoid;//

	private java.math.BigDecimal AdditionalNumberFlag;//

	private java.math.BigDecimal BizResourceId;//

	private java.util.Date RegisteredExpiredAt;//

	private String OdsId;//

	private Short OdsState;//

	private String Id;//

	private java.util.Date OdsTimestamp;//

	private java.util.Date OdsCreatedate;//
	
	private Long ts;
	
	public Long getTs() {
		return this.RegisteredAt.getTime();
	}
	
	
}
