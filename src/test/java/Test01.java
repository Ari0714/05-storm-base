import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.ResultSet;

public class Test01 {

    public static void main(String[] args) throws IOException {

//        BufferedReader gg = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\02storm\\原数据\\lng-lat-mapping.txt"));
////
////        String aa;
////
////        while ((aa = gg.readLine()) != null)
////            System.out.println(aa);

        String aa = "SELECT lng,lat from lng_lat_mapping where city LIKE \"" + 111 + "\" ";

        String[] strings = aa.split("_");

//        String ipNUm = strings[3].replaceAll("市", "");

//        System.out.println(strings[3]);
        System.out.println(aa);

    }

}
