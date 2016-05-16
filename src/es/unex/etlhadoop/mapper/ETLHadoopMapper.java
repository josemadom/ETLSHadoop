package es.unex.etlhadoop.mapper;

import java.io.IOException;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import es.unex.etlhadoop.utils.Constantes;

public class ETLHadoopMapper  extends Mapper<LongWritable, Text, Point, Text> 
{

	private static final int MISSING = 9999;
	
	
	public void mapSpatial(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException 
	{
		String Cadena = "";

		final String[] values = value.toString().split(",");

		final String fecha = format(values[0]).substring(0,7);      
		final String lat= format(values[1]); 
		final String latNeg= format(values[1]).substring(0,3); 

		final String lon= format(values[2]);   
		
		Double dLat = Double.parseDouble(lat);
		Double dLatNeg = Double.parseDouble(latNeg);		
		Double dLon = Double.parseDouble(lon);		
		
		Point p = new Point(dLat,dLon);
		Point pLatNeg = new Point(dLatNeg,dLon);		

		final String ws = format(values[3]);
		final String wd = format(values[4]);
		
		//Definimos las zonas que nos interesan
		Rectangle r1 = new Rectangle(Constantes.R1X, Constantes.R1Y,Constantes.R1ancho , Constantes.R1alto);
		Rectangle r2 = new Rectangle(Constantes.R2X, Constantes.R2Y,Constantes.R2ancho , Constantes.R2alto);
		Rectangle r3 = new Rectangle(Constantes.R3X, Constantes.R3Y,Constantes.R3ancho , Constantes.R3alto);
		Rectangle r4 = new Rectangle(Constantes.R4X, Constantes.R4Y,Constantes.R4ancho , Constantes.R4alto);		

		if ( NumberUtils.isNumber(fecha.toString()) )
		{  
			//Region 1			
			if (r1.contains(p)){
				Text t = new Text();
				Cadena = ws.concat(",").concat(wd);
				t.set(Cadena);
				context.write(p, new Text(t));
			}else if (r1.contains(pLatNeg)){
				Text t = new Text();
				Cadena = ws.concat(",").concat(wd);
				t.set(Cadena);
				context.write(pLatNeg, new Text(t));
			}
			
			//Region 2
			if (r2.contains(p)){
				Text t = new Text();
				Cadena = ws.concat(",").concat(wd);
				t.set(Cadena);
				context.write(p, new Text(t));
			}else if (r2.contains(pLatNeg)){
				Text t = new Text();
				Cadena = ws.concat(",").concat(wd);
				t.set(Cadena);
				context.write(pLatNeg, new Text(t));
			}
			
			//Region 3
			if (r3.contains(p)){
				Text t = new Text();
				Cadena = ws.concat(",").concat(wd);
				t.set(Cadena);
				context.write(p, new Text(t));
			}else if (r3.contains(pLatNeg)){
				Text t = new Text();
				Cadena = ws.concat(",").concat(wd);
				t.set(Cadena);
				context.write(pLatNeg, new Text(t));
			}
			
			//Region 4
			if (r4.contains(p)){
				Text t = new Text();
				Cadena = ws.concat(",").concat(wd);
				t.set(Cadena);
				context.write(p, new Text(t));
			}else if (r4.contains(pLatNeg)){
				Text t = new Text();
				Cadena = ws.concat(",").concat(wd);
				t.set(Cadena);
				context.write(pLatNeg, new Text(t));
			}			
		}	

	}
	private String format(String value) {
		return value.trim();
	}

}

