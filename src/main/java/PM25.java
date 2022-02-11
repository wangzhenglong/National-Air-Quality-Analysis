import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Description TODO
 * author dragonKJ
 * createTime 2022/2/10  14:06
 */
public class PM25 {

    public static void main(String[] args) {
        //获取站点集合信息
        HashMap<String,String> siteMap=site();
        // 设置转换格式
        DecimalFormat df = new DecimalFormat("0.00%");
        Long start= new Date().getTime();
        //年份
        String year="2022";
        //导出文件名称
        String fileName="C:\\Users\\admin\\Desktop\\全国空气\\PM2.5-over150\\PM2.5-over150-"+year+".csv";
        //csv所在文件夹
        String BasePath = "C:\\Users\\admin\\Desktop\\全国空气\\全国空气质量数据\\站点_20220101-20220205";
        Path dir = Paths.get(BasePath);
        //创建一个set来收集处理的结果
        HashSet<AirClass> set=new HashSet<>();
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
                                airClass=new AirClass();
                                Double value=0d;
                                //如果站点该类型数据为空，舍弃该数据
                                if(!valuesArray[i].equals(""))
                                {
                                    value=Double.valueOf(valuesArray[i]);
                                }else {
                                    continue;
                                }
                                //将数据封装成对象装入set
                                airClass.setDate(valuesArray[0]);
                                airClass.setHour(valuesArray[1]);
                                airClass.setType(valuesArray[2]);
                                airClass.setSite(keys[i]);
                                airClass.setNum(value);
                                set.add(airClass);
                            }

                        });
                    }
            );

        }catch (IOException e){
            System.out.println(e.toString());
        }

        //将数据按站点分组
        Map<String, List<AirClass>> map=set.parallelStream()
                //数据按site分组 --key
                .collect(Collectors.groupingBy(AirClass::getSite));



        ArrayList<String> arrayList=new ArrayList();

        map.entrySet().stream().forEach(entry->{
            //PM2.5总的结果集
            List<AirClass> airClassList=entry.getValue();
            //获取PM2.5大于150的结果集
            List<AirClass> airClassList2=
                    airClassList.stream().filter(airClass->airClass.getNum()>150).collect(Collectors.toList());
            //计算PM2.5大于150的比例
            Double size=Double.valueOf(airClassList2.size())/Double.valueOf(airClassList.size());

            //组装站点，比例-返回结果集
            arrayList.add(year+","+entry.getKey()+","+siteMap.get(entry.getKey())+","+df.format(size));
        });
        Collections.sort(arrayList);
        writeCsv(fileName,arrayList);
        Long end= new Date().getTime();
        Long time=end-start;
        System.out.println("耗时"+time/1000+"秒，"+time%1000+"毫秒");

    }
    //写入csv文件
    public static void writeCsv(String fileName, ArrayList<String> arrayList){
        File csvOutputFile=new File(fileName);
        try(PrintWriter pw = new PrintWriter(csvOutputFile);){
            pw.println("年度,监测点编码,监测点名称,城市,PM2.5超过150的时间占比");
            arrayList.forEach(str->
                            pw.println(str)
                    );

        }catch (IOException e){
            System.out.println(e.toString());
        }

    }

    //获取站点集合
    public  static HashMap<String,String> site(){
        //存储所有站点数据信息
        HashMap<String,String>  keyMap=new HashMap<>();
        Long start= new Date().getTime();
        //csv所在文件夹
        String BasePath = "C:\\Users\\admin\\Desktop\\全国空气\\_站点列表";
        Path dir = Paths.get(BasePath);
        LinkedHashMap<String,HashSet> hashMap=new LinkedHashMap<>();
        //获取文件列表
        try (Stream<Path> files1 = Files.list(dir);){
            //文件-并行-批量处理
            files1.forEach(file->{
                        //判断文件是否csv文件
                        if(!file.getFileName().toString().endsWith(".csv"))
                        {return;}
                        String date=file.getFileName().toString().replace("站点列表-","")
                                .replace("起.csv","");
                        List<String> lines = null;
                        try {
                            //按行读取
                            lines = Files.readAllLines(file, Charset.forName("utf8"));
                        } catch (IOException exception) {
                            exception.printStackTrace();
                        }
                        //处理文件的数据，从第二行开始
                        List<String> lines2=lines.stream().skip(1).collect(Collectors.toList());
                        //创建一个set来收集处理的结果
                        LinkedHashSet<String> set=new LinkedHashSet<>();
                        //循环处理每一行数据
                        lines2.stream().forEach(values->{
                            //将每一行数据分割
                            String valuesArray[]=values.split(",",-1);
                            //将第数据加入set（站点信息）
                            set.add(valuesArray[0]);
                            keyMap.put(valuesArray[0],valuesArray[1]+","+valuesArray[2]);

                        });

                        hashMap.put(date,set);

                    }
            );



        }catch (IOException e){
            System.out.println(e.toString());
        }
        return keyMap;
    }

}
