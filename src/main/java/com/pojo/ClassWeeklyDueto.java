package com.pojo;

import java.io.Serializable;
import java.util.Date;

public class ClassWeeklyDueto implements Serializable {

	private static final long serialVersionUID = 1L;
	public String Outlet;
	public String Catlib;
	public String ProdKey;
	public double Geogkey;
	public Date Week;
	public String SalesComponent;
	public double Dueto_value;
	public String PrimaryCausalKey;
	public double Causal_value;
	public String Country;
	public String Iteration;
	public String SourceBDA;
	public String getOutlet() {
		return Outlet;
	}
	public void setOutlet(String outlet) {
		Outlet = outlet;
	}
	public String getCatlib() {
		return Catlib;
	}
	public void setCatlib(String catlib) {
		Catlib = catlib;
	}
	public String getProdKey() {
		return ProdKey;
	}
	public void setProdKey(String prodKey) {
		ProdKey = prodKey;
	}
	public double getGeogkey() {
		return Geogkey;
	}
	public void setGeogkey(double geogkey) {
		Geogkey = geogkey;
	}
	public Date getWeek() {
		return Week;
	}
	public void setWeek(Date week) {
		Week = week;
	}
	public String getSalesComponent() {
		return SalesComponent;
	}
	public void setSalesComponent(String salesComponent) {
		SalesComponent = salesComponent;
	}
	public double getDueto_value() {
		return Dueto_value;
	}
	public void setDueto_value(double dueto_value) {
		Dueto_value = dueto_value;
	}
	public String getPrimaryCausalKey() {
		return PrimaryCausalKey;
	}
	public void setPrimaryCausalKey(String primaryCausalKey) {
		PrimaryCausalKey = primaryCausalKey;
	}
	public double getCausal_value() {
		return Causal_value;
	}
	public void setCausal_value(double causal_value) {
		Causal_value = causal_value;
	}
	public String getCountry() {
		return Country;
	}
	public void setCountry(String country) {
		Country = country;
	}
	public String getIteration() {
		return Iteration;
	}
	public void setIteration(String iteration) {
		Iteration = iteration;
	}
	public String getSourceBDA() {
		return SourceBDA;
	}
	public void setSourceBDA(String sourceBDA) {
		SourceBDA = sourceBDA;
	}
	
	
	
		
}