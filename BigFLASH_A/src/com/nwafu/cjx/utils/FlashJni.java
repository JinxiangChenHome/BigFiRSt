package com.nwafu.cjx.utils;

import com.nwafu.cjx.common.ClientInfo;
/**
 * author: Jinxiang Chen
 * mail: 631493049@163.com
 * 
 */
public class FlashJni implements ClientInfo{
	static {
        System.load(FLASH_SO);
    }  
    private native int flash_jni(int argc, String[] argv);  
    public static int Flash_Jni(String[] args) {  
    	int returnCode = new FlashJni().flash_jni(args.length,args);
        System.out.println("returnCode = "+returnCode);
    	return returnCode;
    }  
}
