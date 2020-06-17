package cn.learn.gulietl;

import org.omg.CORBA.StringHolder;

public class ETLUTil {

    public static String oriString2UtilString(String oriStr){

        StringBuilder etlStr = new StringBuilder();
        String[] fields = oriStr.split("\t");
        if (fields.length < 9) return null;
        fields[3] = fields[3].replaceAll(" ","");
        for (int i = 0; i < fields.length; i++) {
            if (i < 9){
               etlStr.append(fields[i] + "\t");
            }else {
                etlStr.append(fields[i] + "&");

            }
        }
       etlStr.deleteCharAt(etlStr.length()-1);
        return etlStr.toString();
    }


}
