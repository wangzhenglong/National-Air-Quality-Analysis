import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Description PM2.5分析-新版-平均浓度-成渝-按季节分组
 * author dragonKJ
 * createTime 2022/2/10  14:06
 */
public class PM25New_cy_season {

    public static void main(String[] args) {
        //获取站点集合信息
        HashMap<String,String> siteMap=site();

        Long start= new Date().getTime();
        //年份
        String year="2021";
        //导出文件名称
        String fileName="C:\\Users\\admin\\Desktop\\全国空气\\PM2.5-cy-season\\PM2.5-"+year+".csv";
        //csv所在文件夹
        String BasePath = "C:\\Users\\admin\\Desktop\\全国空气\\全国空气质量数据\\站点_20210101-20211231";
        Path dir = Paths.get(BasePath);
        //创建一个ArrayList来收集处理的结果-这里使用list总时间比set快一倍
        ArrayList<AirClass> airClassArrayList=new ArrayList<>();
        //获取文件列表
        try (Stream<Path> files1 = Files.list(dir);){
            //文件-并行-批量处理
            files1.parallel().forEach(file->{
                        //判断文件是否csv文件
                        if(!file.getFileName().toString().endsWith(".csv"))
                        {return;}
                        List<String> lines = null;
                        try {
                            //按行读取
                            lines = Files.readAllLines(file, Charset.forName("gbk"));
                        } catch (IOException exception) {
                            exception.printStackTrace();
                        }
                        //获取文件第一行，字段标题
                        String keys[]=lines.get(0).split(",",-1);
                        //处理文件的数据，从第二行开始
                        List<String> lines2=lines.stream().skip(1).collect(Collectors.toList());

                        //循环处理每一行数据
                        lines2.stream().forEach(values->{
                            //将每一行数据分割
                            String valuesArray[]=values.split(",",-1);
                            //判断type取PM2.5
                            if(valuesArray.length<2||!valuesArray[2].equals("PM2.5")){
                                return;
                            }
                            AirClass airClass;
                            //循环每一列的数据集，处理每个站点
                            for(int i=3;i<valuesArray.length;i++){
                                //过滤不是成渝的站点
                                if(!siteMap.containsKey(keys[i])){
                                    continue;
                                }
                                airClass=new AirClass();
                                Double value=0d;
                                //如果站点该类型数据为空，舍弃该数据
                                if(!valuesArray[i].equals(""))
                                {
                                    value=Double.valueOf(valuesArray[i]);
                                }else {
                                    continue;
                                }
                                //将数据封装成对象装入list
                                airClass.setDate(valuesArray[0]);
                                airClass.setHour(valuesArray[1]);
                                airClass.setType(valuesArray[2]);
                                airClass.setSite(keys[i]);
                                airClass.setNum(value);
                                airClass.setCity(siteMap.get(keys[i]));
                                int day=Integer.valueOf(valuesArray[0].substring(4,6));
                                if(3<=day&&day<=5){
                                    airClass.setSeason("春");
                                }else if(6<=day&&day<=8){
                                    airClass.setSeason("夏");
                                }else if(9<=day&&day<=11){
                                    airClass.setSeason("秋");
                                }else if(12<=day||day<=2){
                                    airClass.setSeason("冬");
                                }else{
                                    System.out.println("日期格式错误");
                                }
                                airClassArrayList.add(airClass);
                            }

                        });
                    }
            );

        }catch (IOException e){
            System.out.println(e.toString());
        }

        //将数据按站点分组
        Map<String, Double> map=airClassArrayList.parallelStream()
                //数据按site分组 --key
                .collect(Collectors.groupingBy((airClass -> airClass.getCity()+","+airClass.getSeason()),
                        Collectors.averagingDouble(AirClass::getNum)));



        List<AirClass> arrayList=Collections.synchronizedList(new ArrayList());

        map.entrySet().parallelStream().forEach(entry->{
            String arr[]=entry.getKey().split(",",-1);
            AirClass airClass=new AirClass();
            airClass.setSeason(arr[2]);
            airClass.setCity(arr[0]+","+arr[1]);
            airClass.setNum(entry.getValue());
           //组装站点，比例-返回结果集
            arrayList.add(airClass);
        });
        Map<String,List<AirClass>> mapCity=arrayList.stream().collect(Collectors.groupingBy(AirClass::getCity));
        List<String> arrayList2=Collections.synchronizedList(new ArrayList());
        mapCity.entrySet().stream().forEach(entry->{
            List<AirClass> airClasses=entry.getValue();
            HashMap<String,Double> hashMap=new HashMap();
            airClasses.stream().forEach(air->{
                hashMap.put(air.getSeason(),air.getNum());
            });
            arrayList2.add(year+","+entry.getKey()+","+hashMap.get("春").toString()+","+hashMap.get("夏").toString()+","+hashMap.get("秋").toString()+","+hashMap.get("冬").toString());
        });
        writeCsv(fileName,arrayList2);
        Long end= new Date().getTime();
        Long time=end-start;
        System.out.println("耗时"+time/1000+"秒，"+time%1000+"毫秒");

    }
    //写入csv文件
    public static void writeCsv(String fileName, List<String> arrayList){

        Collections.sort(arrayList);
        File csvOutputFile=new File(fileName);
        try(PrintWriter pw = new PrintWriter(csvOutputFile);){
            //,污染平均持续时间(小时),年度检测总时间(小时)
            pw.println("年度,省/市,市/区,春,夏,秋,冬");
            arrayList.forEach(str->
                            pw.println(str)
                    );

        }catch (IOException e){
            System.out.println(e.toString());
        }

    }

    //获取站点-区域集合
    public  static HashMap<String,String> site(){
        //存储所有站点数据信息
        HashMap<String,String>  keyMap=new HashMap<>();
        //csv所在文件夹
        String BasePath = "C:\\Users\\admin\\Desktop\\全国空气\\站点列表-成渝.csv";
        Path dir = Paths.get(BasePath);
        List<String> lines = null;
        try {
            //按行读取
            lines = Files.readAllLines(dir, Charset.forName("utf8"));
        } catch (IOException exception) {
            exception.printStackTrace();
        }
        //循环处理每一行数据
        lines.stream().forEach(values->{
            //将每一行数据分割
            String valuesArray[]=values.split(",",-1);
            keyMap.put(valuesArray[0],valuesArray[3]+","+valuesArray[2]);

        });

        return keyMap;
    }

}
