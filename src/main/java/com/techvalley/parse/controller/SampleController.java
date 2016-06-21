package com.techvalley.parse.controller;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * @author wuyq
 * 
 */
@Controller
@RequestMapping("/sample")
public class SampleController {
	/**
	 * 
	 * @param param
	 * @return
	 */
	@RequestMapping(value = "/helloworld", method = RequestMethod.POST, produces = { "application/json;charset=utf8" })
	@ResponseBody
	public String listHost(@RequestParam int param) {
		return "hello world";
	}
	@Autowired
	private ThreadPoolTaskExecutor taskExecutor;
	
	/**
	 * 
	 * @param param
	 * @return
	 * @throws ClassNotFoundException 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 * @throws IOException 
	 */
	@RequestMapping(value = "/threadpool", method = RequestMethod.POST, produces = { "application/json;charset=utf8" })
	@ResponseBody
	public String threadpoolTest() throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException{
		taskExecutor.execute(new Runnable(){
			
			public void run() {
				System.out.println("thread1");
			}
			
		});
		taskExecutor.execute(new Runnable(){

			public void run() {
				System.out.println("thread2");
			}
			
		});
		taskExecutor.execute(new Runnable(){

			public void run() {
				System.out.println("thread3");
			}
			
		});
		taskExecutor.execute(new Runnable(){

			public void run() {
				System.out.println("thread4");
			}
			
		});
		System.out.println("corePoolSize="+taskExecutor.getCorePoolSize());
		System.out.println("ActiveCount="+taskExecutor.getActiveCount());
		System.out.println("poolSize="+taskExecutor.getPoolSize());
		return "success";
	}
	
	
}
