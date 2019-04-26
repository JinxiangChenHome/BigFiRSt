package com.nwafu.cjx.flash;

import com.nwafu.cjx.utils.FlashJni;
/**
 * author: Jinxiang Chen
 * mail: 631493049@163.com
 * 
 */
public class FlashSeq {
	public static void main(String[] args) {
		int returnCode = new FlashJni().Flash_Jni(args);
        System.out.println("returnCode = "+returnCode);
	}
}
