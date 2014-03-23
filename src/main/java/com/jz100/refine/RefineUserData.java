package com.jz100.refine;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Map;

import com.jz100.util.CommonUtil;
import com.jz100.util.Props;



public class RefineUserData {
	
	CommonUtil cu = new CommonUtil();
	private static final String input = "data1.txt";
	private static final String vector_output = "vector.out";
	private static final String label_output = "label.out";
	private static final String train_vector_output = "train_vector.out";
	private static final String test_vector_output = "test_vector.out";
	private static final String train_label_output = "train_label.out";
	private static final String test_label_output = "test_label.out";
	
	
	
	public static void main(String[] args) throws Exception {
		long startTime=System.currentTimeMillis();   //获取开始时间  
		RefineUserData refineUserData = new RefineUserData();
		refineUserData.run();
		long endTime=System.currentTimeMillis(); //获取结束时间
		
		System.out.println("程序运行时间： "+(endTime-startTime)+"ms");
	}
	
	
	public void run() throws Exception {
		
		Props props = new Props("default.props");
		Map<String, String> propMap = props.getProps();
		String trainInclude[] = propMap.get("trainInclude").split(CommonUtil.COMMA);
		String testExclude[] = propMap.get("testExclude").split(CommonUtil.COMMA);
		
		
		BufferedReader br = new BufferedReader(new FileReader(input));
		StringBuilder sb = new StringBuilder();
		FileWriter vector_fw = new FileWriter(vector_output);
		FileWriter label_fw = new FileWriter(label_output);
		FileWriter train_vector_fw = new FileWriter(train_vector_output);
		FileWriter test_vector_fw = new FileWriter(test_vector_output);
		FileWriter train_label_fw = new FileWriter(train_label_output);
		FileWriter test_label_fw = new FileWriter(test_label_output);
		//DenseVector dv = new DenseVector();
		String line = null;
		String newline = null;
		String labelId;
		double[] min = new double[34];
		for(int i=0;i<34;i++) {
			min[i] = 0.0;
		}
		double[] max = new double[34];
		for(int i=0;i<34;i++) {
			max[i] = 0.0;
		}
		
		Double oldValue, newValue;
		int j = 0;
		while((line = br.readLine()) != null) {
			String[] terms = line.split(CommonUtil.TAB);
			if(terms.length > 45) {
				for(int i=0;i<34;i++) {
					oldValue = Double.valueOf(cu.setEmpty2Zero(terms[i]));
					if(j == 0) {
						min[i] = max[i] = oldValue;
					}
					if(oldValue > max[i]) {
						max[i] = oldValue;
					}
					if(oldValue < min[i]) {
						min[i] = oldValue;
					}
				}
				j++;
			}
		}
		br.close();
		
		
		
		br = new BufferedReader(new FileReader(input));
		while((line = br.readLine()) != null) {
			String[] terms = line.split(CommonUtil.TAB);
			if(terms.length > 45) {
				sb.setLength(0);
				for(int i=0;i<34;i++) {
					oldValue = Double.valueOf(cu.setEmpty2Zero(terms[i]));
					newValue = (oldValue-min[i])/(max[i]-min[i]);	//归一化处理
					
					
					sb.append("\t").append(String.valueOf(newValue));
				}
				newline = sb.append("\n").toString().substring(1);
				vector_fw.write(newline);
				labelId = cu.setEmpty2Zero(terms[44]);
				
				boolean isContinue = true;
				boolean isWritable = true;
				//见习版主/版主/超级版主 train.props
				for(j=0;j<trainInclude.length;j++) {
					if(trainInclude[j].equals(labelId)) {
						train_vector_fw.write(newline);
						train_label_fw.write(labelId+"\n");
						isContinue = false;
						break;
					}
				}
				
				//排除管理员等 test.props
				if(isContinue) {
					for(j=0;j<testExclude.length;j++) {
						if(testExclude[j].equals(labelId)) {
							isWritable = false;
							break;
						}
					}
					if(isWritable) {
						test_vector_fw.write(newline);
						test_label_fw.write(labelId+"\n");
					}
				}
				
				
				sb.setLength(0);
				newline = sb.append(labelId).append("\n").toString();
				label_fw.write(newline);
			}
		}
		
		br.close();
		
		vector_fw.close();
		label_fw.close();
		train_vector_fw.close();
		test_vector_fw.close();
		train_label_fw.close();
		test_label_fw.close();
		
		
		
		
	}
	
	
	
	
	
	
	
	
	
	
	
}
