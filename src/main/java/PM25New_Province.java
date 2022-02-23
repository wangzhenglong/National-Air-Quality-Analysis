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
 * Description PM2.5分析-新版-百分比-按省
 * author dragonKJ
 * createTime 2022/2/10  14:06
 */
public class PM25New_Province {

    public static void main(String[] args) {
        //获取站点集合信息
        HashMap<String,String> siteMap=site();

        Long start= new Date().getTime();
        //年份
        String year="2022";
        //导出文件名称
        String fileName="C:\\Users\\admin\\Desktop\\全国空气\\PM2.5-Province\\PM2.5-"+year+".csv";
        //csv所在文件夹
        String BasePath = "C:\\Users\\admin\\Desktop\\全国空气\\全国空气质量数据\\站点_20220101-20220205";
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
                                airClassArrayList.add(airClass);
                            }

                        });
                    }
            );

        }catch (IOException e){
            System.out.println(e.toString());
        }

        //将数据按站点分组
        Map<String, List<AirClass>> map=airClassArrayList.parallelStream()
                //数据按site分组 --key
                .collect(Collectors.groupingBy(AirClass::getSite));



        List<String> arrayList=Collections.synchronizedList(new ArrayList());

        map.entrySet().parallelStream().forEach(entry->{
            //PM2.5总的结果集
            List<AirClass> airClassList=entry.getValue();
            //获取PM2.5大于150的结果集-重度污染
            List<AirClass> airClassList2=
                    airClassList.stream().filter(airClass->airClass.getNum()>150).collect(Collectors.toList());
            //获取PM2.5大于35的结果集-污染
            List<AirClass> airClassList3=
                    airClassList.stream().filter(airClass->airClass.getNum()>35).collect(Collectors.toList());
            //获取PM2.5小于等于35的结果集-优良
            List<AirClass> airClassList4=
                    airClassList.stream().filter(airClass->airClass.getNum()<=35).collect(Collectors.toList());

            //计算PM2.5大于150的比例
            Double size2=Double.valueOf(airClassList2.size())==0?0:Double.valueOf(airClassList2.size())/Double.valueOf(airClassList.size());
            //计算PM2.5大于35的比例
            Double size3=Double.valueOf(airClassList3.size())==0?0:Double.valueOf(airClassList3.size())/Double.valueOf(airClassList.size());
            //计算PM2.5小于等于35的比例
            Double size4=Double.valueOf(airClassList4.size())==0?0:Double.valueOf(airClassList4.size())/Double.valueOf(airClassList.size());


            //获取污染平均持续时间
            AtomicInteger count=new AtomicInteger(0);
            //是否连续污染
            AtomicBoolean flag= new AtomicBoolean(false);
            airClassList.stream().forEach(airClass->{
                if(airClass.getNum()>35&& !flag.get()){
                    count.incrementAndGet();
                    flag.set(true);
                }else if(airClass.getNum()>35&& flag.get()){
                    flag.set(true);
                }else{
                    flag.set(false);
                }
            });
            //计算污染平均持续时间
            Double durationAVG=Double.valueOf(airClassList3.size())==0?0:Double.valueOf(airClassList3.size())/count.get();
            //组装站点，比例-返回结果集
            arrayList.add(year+","+entry.getKey()+","+siteMap.get(entry.getKey())+","+size2+","+size3+","+size4+","+durationAVG);
        });
        Collections.sort(arrayList);
        writeCsv(fileName,arrayList);
        Long end= new Date().getTime();
        Long time=end-start;
        System.out.println("耗时"+time/1000+"秒，"+time%1000+"毫秒");

    }
    //写入csv文件
    public static void writeCsv(String fileName, List<String> arrayList){
        // 设置转换格式
        DecimalFormat df = new DecimalFormat("0.00%");
        //按区域分组
        Map<String, List<String[]>> map=arrayList.parallelStream().map(str->str.split(",",-1))
                .collect(Collectors
                        .groupingBy(arrys->arrys[2]));
        //求每个分组的平均值
        List resList=map.entrySet().stream().map(entry->{
            Double d1= entry.getValue().stream().collect(Collectors.averagingDouble(list->Double.valueOf(list[4])));
            Double d2= entry.getValue().stream().collect(Collectors.averagingDouble(list->Double.valueOf(list[5])));
            Double d3= entry.getValue().stream().collect(Collectors.averagingDouble(list->Double.valueOf(list[6])));
            Double d4= entry.getValue().stream().collect(Collectors.averagingDouble(list->Double.valueOf(list[7])));
            return entry.getValue().get(0)[0]+","+entry.getValue().get(0)[2]+","+df.format(d1)+","+df.format(d2)+","+df.format(d3)+","+d4;
        }).collect(Collectors.toList());
        Collections.sort(resList);
        File csvOutputFile=new File(fileName);
        try(PrintWriter pw = new PrintWriter(csvOutputFile);){
            //,污染平均持续时间(小时),年度检测总时间(小时)
            pw.println("年度,省/市,PM2.5>150的时间占比,PM2.5>35的时间占比,PM2.5<=35的时间占比,污染平均持续时间(小时)");
            resList.forEach(str->
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
        String BasePath = "C:\\Users\\admin\\Desktop\\全国空气\\站点列表-三大城市群.csv";
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
