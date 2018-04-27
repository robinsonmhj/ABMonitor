/**
 * 
 *//*
package com.tmg.AOP;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.AfterThrowing;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;

*//**
 * @author Haojie Ma
 * @date Sep 15, 2015
 *//*

@Aspect
public class TestAOP {
	
	
	@Around("within(com.tmg.greenplum.DAOImp.GreenplumDAOImp)")
    public Object  logExceptions(ProceedingJoinPoint proceedingJoinPoint){
		System.out.println("Before invoking getName() method");
        Object value = null;
        try {
            value = proceedingJoinPoint.proceed();
        } catch (Throwable e) {
            e.printStackTrace();
        }
        System.out.println("After invoking getName() method. Return value="+value);
        return value;
    }
	
	
	
	@AfterThrowing(pointcut = "execution(* com.tmg.greenplum.DAOImp..* (..))", throwing = "ex")
	public void errorInterceptor(Exception  ex) {

		

	    System.out.println("Its me\n");
	    ex.printStackTrace();


	}
	

}


*/