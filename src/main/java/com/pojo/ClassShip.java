package com.pojo;

import java.io.Serializable;

public class ClassShip implements Serializable {

	private static final long serialVersionUID = 1L;
	public String Brand;
	public String Beneficiary;
	public String Catlib;
	public String Channel;
	public String Period;
	public Double ChannelVolume;
	public Double AllOutletVolume;
	public Double ProjectionFactor;
	public String getBrand() {
		return Brand;
	}
	public void setBrand(String brand) {
		Brand = brand;
	}
	public String getBeneficiary() {
		return Beneficiary;
	}
	public void setBeneficiary(String beneficiary) {
		Beneficiary = beneficiary;
	}
	public String getCatlib() {
		return Catlib;
	}
	public void setCatlib(String catlib) {
		Catlib = catlib;
	}
	public String getChannel() {
		return Channel;
	}
	public void setChannel(String channel) {
		Channel = channel;
	}
	public String getPeriod() {
		return Period;
	}
	public void setPeriod(String period) {
		Period = period;
	}
	public Double getChannelVolume() {
		return ChannelVolume;
	}
	public void setChannelVolume(Double channelVolume) {
		ChannelVolume = channelVolume;
	}
	public Double getAllOutletVolume() {
		return AllOutletVolume;
	}
	public void setAllOutletVolume(Double allOutletVolume) {
		AllOutletVolume = allOutletVolume;
	}
	public Double getProjectionFactor() {
		return ProjectionFactor;
	}
	public void setProjectionFactor(Double projectionFactor) {
		ProjectionFactor = projectionFactor;
	}

	
}
