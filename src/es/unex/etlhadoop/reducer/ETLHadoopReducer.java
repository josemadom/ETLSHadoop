package es.unex.etlhadoop.reducer;
// cc MaxTemperatureReducer Reducer for maximum temperature example
// vv MaxTemperatureReducer
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ETLHadoopReducer  extends Reducer<Text, Text, Text, Text> 
{
  
  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException 
  {
      float windD=0, windS=0, div=0, division=0, sumaWD=0, sumaWS=0, mWD, mWS;
      int cont=0;
	  Text t = new Text();
	  String Cadena = "";
	  
	  String[] linea;	  
      String wss, wdd; 
      
	  for (Text v : values)
	  {
		  linea = v.toString().split(",");		  
	      wss = format(linea[0]);      
	      wdd= format(linea[1]);  
	      
	      windS = Float.parseFloat(wss);
	      sumaWS = sumaWS+windS;
	      
	      windD = Float.parseFloat(wdd);	
	      sumaWD = sumaWD+windD;
	     
	      cont=cont+1;
	  }
	  
	  mWD=sumaWD/cont;
	  mWS=sumaWS/cont;
	  
	  if (mWS==0)
      {
    	  division=9999;
      }
      else
      {
    	  division=mWD/mWS;
      } 
      
	  String DSR = Float.toString(division);
	  String mediaWD = Float.toString(mWD);
	  String mediaWS = Float.toString(mWS);
	  
	  Cadena=Cadena.concat(",").concat(mediaWS).concat(",").concat(mediaWD).concat(",").concat(DSR);
      
	  t.set(Cadena);
	  context.write(key, t);
  }
  
  private String format(String value) {
      return value.trim();
  }
}
