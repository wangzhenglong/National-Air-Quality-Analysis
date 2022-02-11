import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Description 站点数据处理-随日期增加、减少-站点数
 * author dragonKJ
 * createTime 2022/2/10  10:31
 */
public class Site {
    public static void main(String[] args) {
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
            //新建list存储日期
            ArrayList<String> list1=new ArrayList<>();
            //新建list存储各个时间的站点
            ArrayList<LinkedHashSet> list2=new ArrayList<>();
            //循环将日期装入list1，站点数据装入list2
            for (Map.Entry entry:hashMap.entrySet()) {
                list1.add(entry.getKey().toString());
                list2.add((LinkedHashSet)entry.getValue());
            }
            //对比前后两个日期站点数据，取得后一个日期比前一个日期站点增加的数据
            LinkedHashMap<String,HashSet> hashMap1=new LinkedHashMap<>();
            for(int i=1;i<list2.size();i++){
                LinkedHashSet<String> set1=(LinkedHashSet<String>)list2.get(i).clone();
                LinkedHashSet<String> set2=(LinkedHashSet<String>)list2.get(i-1).clone();
                set1.removeAll(set2);
                hashMap1.put(list1.get(i),set1);
            }
            //对比前后两个日期站点数据，取得后一个日期比前一个日期站点减少的数据
            LinkedHashMap<String,HashSet> hashMap2=new LinkedHashMap<>();
            for(int i=1;i<list2.size();i++){
                LinkedHashSet<String> set1=(LinkedHashSet<String>)list2.get(i).clone();
                LinkedHashSet<String> set2=(LinkedHashSet<String>)list2.get(i-1).clone();
                set2.removeAll(set1);
                hashMap2.put(list1.get(i),set2);
            }
            //写日期-站点新增数据文件
            writeCsv("C:\\Users\\admin\\Desktop\\全国空气\\site-add.csv",hashMap1,keyMap);

            //写日期-站点减少数据文件
            writeCsv("C:\\Users\\admin\\Desktop\\全国空气\\site-sub.csv",hashMap2,keyMap);

        }catch (IOException e){
            System.out.println(e.toString());
        }


        Long end= new Date().getTime();
        Long time=end-start;
        System.out.println("耗时"+time/1000+"秒，"+time%1000+"毫秒");

    }
    //写入csv文件
    public static void writeCsv(String fileName,LinkedHashMap<String,HashSet> hashMap,HashMap keyMap){
        File csvOutputFile=new File(fileName);
        try(PrintWriter pw = new PrintWriter(csvOutputFile);){
            pw.println("日期,监测点编码,监测点名称,城市");
            for (Map.Entry entry:hashMap.entrySet()){
                HashSet set=(HashSet)entry.getValue();
                set.stream().forEach(val->pw.println(entry.getKey()+","+val+","+keyMap.get(val)));
            }

        }catch (IOException e){
            System.out.println(e.toString());
        }

    }
}
