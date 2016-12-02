package com.chinamobile.activemq.queue;

import java.lang.reflect.Field;
import java.util.TimerTask;

public class MyTask extends TimerTask {

	private ActiveQueue queue= null;
	public MyTask(ActiveQueue queue){
		this.queue = queue;
	}
	@Override
	public void run() {
		// TODO Auto-generated method stub
		if(queue!=null){
			queue.delete();
		}
	}

	public void setState(int state) {  
        //缩短周期，执行频率就提高  
        setDeclaredField(TimerTask.class, this, "state", state);  
    }  
      
    //通过反射修改字段的值  
    static boolean setDeclaredField(Class<?> clazz, Object obj,  
            String name, Object value) {  
        try {  
            Field field = clazz.getDeclaredField(name);  
            field.setAccessible(true);  
            field.set(obj, value);  
            return true;  
        } catch (Exception ex) {  
            ex.printStackTrace();  
            return false;  
        }  
    }
}
