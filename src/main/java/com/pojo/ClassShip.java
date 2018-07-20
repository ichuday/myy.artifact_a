package com.pojo;

import java.io.Serializable;

public class ClassShip implements Serializable {

	private static final long serialVersionUID = 1L;
	public String Brand;
	public String Beneficiary;
	public String Catlib;
	public String Channel;
	public String Period;
	public double ChannelVolume;
	public double AllOutletVolume;
	public double ProjectionFactor;
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
	public double getChannelVolume() {
		return ChannelVolume;
	}
	public void setChannelVolume(double channelVolume) {
		ChannelVolume = channelVolume;
	}
	public double getAllOutletVolume() {
		return AllOutletVolume;
	}
	public void setAllOutletVolume(double allOutletVolume) {
		AllOutletVolume = allOutletVolume;
	}
	public double getProjectionFactor() {
		return ProjectionFactor;
	}
	public void setProjectionFactor(double projectionFactor) {
		ProjectionFactor = projectionFactor;
	}

	
}
