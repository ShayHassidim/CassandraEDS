package com.test;

import com.gigaspaces.annotation.pojo.SpaceId;

public class SecondTestData {

	String info;
	Long id;
	
	@SpaceId (autoGenerate = false)
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}

    public String getInfo() {
        return info;
    }

    public void setInfo(String info) {
        this.info = info;
    }
}
