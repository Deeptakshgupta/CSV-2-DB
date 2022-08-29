package com.demo.config;



import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.LineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.PathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import com.demo.listener.Listener;
import com.demo.model.Employee;
import com.demo.repository.EmpRepository;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

	@Autowired
	private DataSource dataSource;
	
	@Autowired 
	private EmpRepository repository;
	
	@Autowired 
	private JobBuilderFactory jobBuilderFractory;
	
	@Autowired
	private StepBuilderFactory stepBuilderFactory;
	
	@Bean
	public FlatFileItemReader<Employee> reader()
	{
		FlatFileItemReader<Employee> reader=new FlatFileItemReader<Employee>();
		
		reader.setResource(new ClassPathResource("/employees.csv"));
		reader.setLineMapper(getLineMapper());
		reader.setLinesToSkip(1);
		
		return reader;
	}

	
	public LineMapper<Employee> getLineMapper() {
		
		DefaultLineMapper<Employee> lineMapper=new DefaultLineMapper<>() ;
		DelimitedLineTokenizer lineTokenizer=new DelimitedLineTokenizer() ;
		BeanWrapperFieldSetMapper<Employee> fieldSetMapper=new BeanWrapperFieldSetMapper<>();
		
		lineMapper.setLineTokenizer(lineTokenizer);
		
		lineTokenizer.setNames(new String[] {"empId","firstName","lastName"});
		lineTokenizer.setIncludedFields(new int[] {0,1,2});
		
		lineMapper.setFieldSetMapper(fieldSetMapper);
		fieldSetMapper.setTargetType(Employee.class);
		return lineMapper;
	}
	
	@Bean
	public EmployeeItemProcessor processor()
	{
		return new EmployeeItemProcessor();
	}
	
	@Bean
	public JdbcTemplate jdbcTemplate(DataSource dataSource) {
		return new JdbcTemplate(dataSource);
	}
	
//	@Bean
//	public DataSource dataSource() throws SQLException {
//		final DriverManagerDataSource dataSource = new DriverManagerDataSource();
//		dataSource.setDriverClassName("com.mysql.cj.mysql.jdbc.Driver");
//		dataSource.setUrl("jdbc:mysql://localhost:3400/batchsystem");
//		dataSource.setUsername("root");
//		dataSource.setPassword("root");
//		return dataSource;
//	}
	
	 @Bean
	    public ItemWriter<Employee> writer(){
	       // return new InvoiceItemWriter(); // Using lambda expression code instead of a separate implementation
	       return emp -> {
	         System.out.println("Saving emp Records: " +emp);
	         repository.saveAll(emp);
	       };
	    }
	
//	 @Bean
//	 public JdbcBatchItemWriter<Employee> writer()
//	 {
//		 JdbcBatchItemWriter<Employee> writer=new JdbcBatchItemWriter<>();
//		 writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Employee>());
//		 writer.setSql("insert into employee(firstName,lastName)values(:firstName,:lastName");
//		 try {
//			writer.setDataSource(dataSource);
//		} catch (Exception e) {
//			System.out.println("Data Source Exception");
//			e.printStackTrace();
//		}
//		 
//		 return writer;
//				
//	 }
	 
	//Listener class Object
	    @Bean
	    public JobExecutionListener listener() {
	       return new Listener();
	    }
	 
	 @Bean
	 public Job importUserJob()
	 {
		 	return this.jobBuilderFractory.get("JOB-1") //JobBuilder
		 			.incrementer(new RunIdIncrementer())
		 			.listener(listener())
		 			.start(step1()) //JobFlowBuilder
		 			.build();

	 }


	 @Bean
	public Step step1() {
		return this.stepBuilderFactory.get("Step1")
				.<Employee,Employee>chunk(10)
				.reader(reader())
				.processor(processor())
					.writer(writer())			
					.build();
	}
}
