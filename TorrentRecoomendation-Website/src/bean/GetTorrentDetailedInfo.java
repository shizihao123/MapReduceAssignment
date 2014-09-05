package bean;

import java.io.IOException;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class GetTorrentDetailedInfo {

	// TODO Auto-generated method stub
	// 获得种子的简介信息
//	public static void main(String[] args) throws IOException {
//		Scanner scanner = new Scanner("40379");
//		while (scanner.hasNextLine()) {
//			String tid = scanner.nextLine();
//			try {
//				System.out.println(tid);
//				TorrentDetailedInfo info = getTorrentDetailedInfo(tid);
//				if (info == null) {
//					System.err.println("fail: " + tid);
//					continue;
//				}
//				System.out.println(info.name + "\n" +info.briefIntroduction + "\n" + info.pictureURL);
//
//			} catch (Exception e) {
//				 e.printStackTrace();
//				System.err.println("fail: " + tid);
//			}
//		}
//
//	}

	public Document getMoviePage(String url) {
		Connection c = Jsoup.connect(url);
		c
				.userAgent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2017.2 Safari/537.36 OPR/24.0.1537.0 (Edition Developer)");
		c.cookie("login", "bsidb");
		c.cookie("uid", "3435");
		c.cookie("md5", "%3D%0CjZ%82%96%D6%2C%01S%3FY%26%3F%B6B");
		c.cookie("per_page", "50");
		// "login="bsidb"; uid="3435"; md5="%3D%0CjZ%82%96%D6%2C%01S%3FY%26%3F%B6B"; per_page="50";
		Connection.Response res = null;
		while (true) {
			try {
				c.timeout(10000);
				res = c.execute();
				break;
			} catch (Exception e) {
				e.printStackTrace();
				System.err.println("we will try it later");
			}
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.err.println("Try again.");
		}
		Document doc = null;
		try {
			doc = res.parse();
		} catch (IOException e) {
			e.printStackTrace();
			System.err.println("Parse fails");
		}
		System.out.println("Got!");
		return doc;
	}

	//获得种子简介的前500个字
	public String getTorrentDetailTopCharacter(Document document) {
        int maxlength = 500;
		Elements e = document.select("tr#notes.file_info");
		Element detailLine = e.first().nextElementSibling();
		Element detail = detailLine.child(1);
        int length = detail.text().length();
        if (length > maxlength) length = maxlength;
		return detail.text().substring(0,length);
	}

    //获得种子标题
    public String getTorrentTitle(Document document) {
        if(document.getElementsByClass("not_exist").size() > 0){
            //该种子并不存在
            return null;//返回null对象
        }
        Elements e = document.select("h3");
        Element titleLine = e.first();
        return titleLine.text();
    }

    //获得种子简介中的第一张图片的url
    public String getTorrentPicture(Document document){
        Elements e = document.select("tr#notes.file_info");
        Element detailLine = e.first().nextElementSibling();
        Element detail = detailLine.child(1);
        Elements imges = detail.getElementsByTag("img");
        if(imges.size() == 0) return "";
        Element firstimg = imges.first();
        if(firstimg == null) return "";

        return firstimg.attr("src");
    }

    //根据TID返回种子的简介信息
    public TorrentDetailedInfo getTorrentDetailedInfo(String tid){
        TorrentDetailedInfo info = new TorrentDetailedInfo();
        Document doc = getMoviePage("http://zijingbt.njuftp.org/stats.html?id=" + tid);
        info.name = getTorrentTitle(doc);
        if(info.name == null) return null;//种子不存在时，返回null对象
        info.briefIntroduction = getTorrentDetailTopCharacter(doc);
        info.pictureURL = getTorrentPicture(doc);
        return info;
    }
}