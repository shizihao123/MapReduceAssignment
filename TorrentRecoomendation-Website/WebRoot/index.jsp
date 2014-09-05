<%@ page language="java" import="java.util.*" pageEncoding="UTF-8"%>
<%
	String path = request.getContextPath();
	String basePath = request.getScheme()+"://"+request.getServerName()+":"+request.getServerPort()+path+"/";
%>

<%@page import="bean.Recommend"%>
<%@page import="bean.GetTorrentDetailedInfo"%>
<%@page import="bean.TorrentDetailedInfo"%>
<%
	String user_id = "";
	String result = "";
    boolean status = false;
    if(request.getMethod() == "POST"){
      	status = true;
      	Recommend recommend = new Recommend();
        user_id = new String(request.getParameter("user_id").getBytes("ISO-8859-1"),"UTF-8");
        result = recommend.recommend(user_id);
    }
    String tresult_id[] = result.split(";");
    String result_id[] = new String[tresult_id.length];
    for(int i = 0;i < result_id.length;i++){
    	String temp[] = tresult_id[i].split(":");
    	result_id[i] = temp[0];
    }
%>
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<html>
	<head>
		<base href="<%=basePath%>">
    	
    	<title>Movie Recommendation</title>
    
		<meta http-equiv="pragma" content="no-cache">
		<meta http-equiv="cache-control" content="no-cache">
		<meta http-equiv="expires" content="0">    
		<meta http-equiv="keywords" content="keyword1,keyword2,keyword3">
		<meta http-equiv="description" content="This is my page">
		<!--
		<link rel="stylesheet" type="text/css" href="styles.css">
		-->
  	</head>

  	<body>
    <center>
        <img src="movie.jpg" width="1000" height="200" alt="logo"/>
        <hr size="1" width="1000">
        <h1 style="color:red">资源推荐</h1>
    </center>
    <br>
    <form name="recommendForm" method="post" action="" >
        <table frame="box" align="center" width="400">
           	<tr>
                <th align="center">请输入用户名：</th>
                <td align="center"><input type="text" name="user_id"/></td>
                <td align="center"><button type="submit" name="recommendButton">猜你喜欢</button></td>
          	</tr>
       	</table>
  	</form>
  	<%
  	if (status){
  		if(result.equals("")) {
    %>
   	<table border="2" align="center" width="600">
      	<tr>
          	<td align="center" style="color: red">推荐结果为空！</td>
      	</tr>
   	</table>
    <%
    	}
    	else {
    %>
    <table border="0" align="center" width="500">
    <tr>
      	<%
      	int count = 0,pp=0;
      	int max = 0;
      	if  (result_id.length > 6) max = 6; else max = result_id.length;
      	while(count < max){
      		GetTorrentDetailedInfo gdti= new GetTorrentDetailedInfo();
      		if(pp >= result_id.length){
      			continue;
      		}else{
		      	TorrentDetailedInfo info = gdti.getTorrentDetailedInfo(result_id[pp]);
		      	if(info == null){ 
		      		pp = pp + 1;
		      	}else{
		      		pp = pp + 1;
		      		count = count + 1;
		      		if(info.pictureURL.equals("")){
	     %>
	      	 		<td align="justify"> <a href="http://zijingbt.njuftp.org/stats.html?id=<%=result_id[pp-1] %>"> <img src="NoPicture.jpg"  width="200" title="<%=info.name %>"> </img> </a></td>
<!-- 	      	 		<td align="justify" style="color: blue"><%out.print(info.briefIntroduction);%></td> -->
	      	 <%  	}else{
	      	 %>
	      	 			
	      	 			<td align="justify"><a href="http://zijingbt.njuftp.org/stats.html?id=<%=result_id[pp-1] %>"><img src="<%=info.pictureURL %>" width="200" title="<%=info.name %>"> </img> </a></td>

<!-- 	      	 			<td align="justify" style="color: blue"><%out.print(info.briefIntroduction);%></td> -->
	      	 			
	      	 <%			 } 
	      	 	}
      	 	}
      	 }
      	  %>
    </tr>  	
   	</table>
    <%
    	}
   	}%>
    </body>
</html>
