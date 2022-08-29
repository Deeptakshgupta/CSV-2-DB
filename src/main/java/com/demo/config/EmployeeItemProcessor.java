package com.demo.config;

import org.springframework.batch.item.ItemProcessor;

import com.demo.model.Employee;

public class EmployeeItemProcessor implements ItemProcessor<Employee,Employee>{

	@Override
	public Employee process(Employee emp) throws Exception {
		// TODO Auto-generated method stub
		return emp;
	}

}
