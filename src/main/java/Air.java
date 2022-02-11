import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Description 全国站点空气质量数据处理导入-日均值
 * author dragonKJ
 * createTime 2022/2/8  16:23
 */
public class Air {

    public static void main(String[] args) {
        Long start= new Date().getTime();
        //csv所在文件夹
        String BasePath = "C:\\Users\\admin\\Desktop\\全国空气\\解压文件目录\\站点_20220101-20220205";
        Path dir = Paths.get(BasePath);
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
                //创建一个set来收集处理的结果
                HashSet<AirClass> set=new HashSet<>();
                //循环处理每一行数据
                lines2.stream().forEach(values->{
                    //将每一行数据分割
                    String valuesArray[]=values.split(",",-1);
                    //判断type是否是以h结尾，如果是-舍弃
                    if(valuesArray.length<2||valuesArray[2].endsWith("h")){
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
                //将数据分组求平均值
                Map<String, Double> map=set.parallelStream()
                        //数据按date,type,site分组 --key
                        .collect(Collectors.groupingBy((air->"\'"+air.getDate()+"\',\'"+air.getType()
                                        +"\',\'"+air.getSite()+"\'"),
                                //数据num按分组求平均值，日均值--value
                                (Collectors.averagingDouble(AirClass::getNum))));
                //将封装好的数据批量插入数据库
                 insert(map);
                }
            );

        }catch (IOException e){
            System.out.println(e.toString());
        }


        Long end= new Date().getTime();
        Long time=end-start;
        System.out.println("耗时"+time/1000+"秒，"+time%1000+"毫秒");

    }

    /**
     * Description 批量插入数据
     * author dragonKJ
     * time 2022/2/9 15:08
     * @param map
     */
    private static void insert(Map<String, Double> map) {
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://127.0.0.1:3307/mytest?useUnicode=true&characterEncoding=utf8";
        String user = "root";
        String password = "root";
        Connection con = null;
        Statement pre = null;
        try {
            Class.forName(driver);
            con = DriverManager.getConnection(url, user, password);
            //设置为不自动提交
            con.setAutoCommit(false);
            pre= con.createStatement();

            String sql="";
            //循环数据集，组装insert语句
            for(Map.Entry entry:map.entrySet()){
               sql="insert into air2022(date,type,site,num)values("+entry.getKey()+","+entry.getValue()+")" ;
                System.out.println(sql);
                //将sql加入批处理
               pre.addBatch(sql);
            }
            //批量执行insert语句
            pre.executeBatch();
            con.commit();


        } catch (Exception e) {
            System.out.println(e.toString());
        } finally {
            try {
                if (pre != null) pre.close();
                if (con != null) con.close();
            } catch (Exception e) {
                System.out.println(e.toString());
            }
        }
    }

}
