<?php 
if(!defined('IN_DISCUZ')) {
	exit('Access Denied');
}
global $_G;
if($_G['uid'] == 176051) {
	//ignore_user_abort(true);
	$time_start = time();
	set_time_limit(0);
	//echo '<pre>';
	//设置文件输出路径
	define("OUTPUT_DIR", './source/plugin/recommend/');
	//获取数据库名
	$db = DB::fetch_first("SELECT DATABASE() AS name");
	$db_name = $db['name'];
//	获取所有post分表
	$post_table_array = array();
	$query = DB::query("SHOW TABLES WHERE Tables_in_".$db_name." REGEXP '^pre_forum_post[_]{0,1}[0-9]*$'");
	while($val = DB::fetch($query)) {
		$post_table_array[] = $val['Tables_in_'.$db_name]; 
	}
	
	
	
	
	//取出用户的统计数据
	$user_query = DB::query("SELECT a.credits,
	b.extcredits1,b.extcredits2,b.extcredits3,b.extcredits4,b.extcredits5,b.extcredits6,b.extcredits7,b.extcredits8,b.extcredits9,
	b.friends,b.posts,b.threads,b.digestposts,b.blogs,b.attachsize,b.views,b.oltime,b.feeds,b.follower,b.following,b.newfollower,
	b.doings,b.albums,b.sharings,
	a.uid,a.avatarstatus,a.regdate,a.groupid,a.adminid,
	c.medals,d.regip,d.lastip,d.lastvisit,d.lastactivity,d.lastpost,d.profileprogress
	FROM ".DB::table('common_member')." AS a 
	LEFT JOIN ".DB::table('common_member_count')." AS b ON a.uid=b.uid 
	LEFT JOIN ".DB::table('common_member_field_forum')." AS c ON a.uid=c.uid
	LEFT JOIN ".DB::table('common_member_status')." AS d ON a.uid=d.uid
	WHERE 1 ORDER BY a.uid ASC");
	//echo OUTPUT_DIR.'title.txt';
	$ftitle = fopen(OUTPUT_DIR.'title.txt', 'wb');
	$fdata = fopen(OUTPUT_DIR.'data.txt', 'wb');
	
	$j = 0;
	while($value = DB::fetch($user_query)) {
		
		
		//设置分表影响的数据
		$divide_keys = array(
			'thread_length' => 0,
			'reply_length' => 0,
			'thread_rate' => 0, 
		);
		
		
		for($i=1;$i<10;$i++) {
			$value['extcredits'.$i] = clean_extcredits($value['extcredits'.$i]); 	
		}
		$value['medals'] = clean_medals($value['medals']);
		$value['regip'] = clean_ip($value['regip']);
		$value['lastip'] = clean_ip($value['lastip']);
		//关联表获取额外统计数据
		$where = "authorid='$value[uid]'";
		//点评数
		$postcomment = getcount('forum_postcomment', $where);
		//主题统计信息,排除特殊主题
		$rs = DB::fetch_first("SELECT 
			AVG(views) AS thread_views, 
			AVG(replies) AS thread_replies, 
			AVG(highlight) AS thread_highlight, 
			AVG(digest) AS thread_digest, 
			AVG(recommends) AS thread_recommends, 
			AVG(heats) AS thread_heats, 
			AVG(favtimes) AS thread_favtimes, 
			AVG(sharetimes) AS thread_sharetimes
			FROM ".DB::table('forum_thread')." WHERE $where AND special=0");
		$rs['postcomment'] = $postcomment;
		$value = array_merge($rs, $value);
		
		foreach($post_table_array as $post_table_name) {
			//主题字数
			$rs = DB::fetch_first("SELECT 
				AVG(CHAR_LENGTH(TRIM(subject))+CHAR_LENGTH(TRIM(message))) AS thread_length
				FROM ".DB::table('forum_post')." WHERE $where AND position=1");
			$divide_keys['thread_length'] += floatval($rs['thread_length']);
			//回复字数
			$rs = DB::fetch_first("SELECT 
				AVG(CHAR_LENGTH(TRIM(subject))+CHAR_LENGTH(TRIM(message))) AS reply_length 
				FROM ".DB::table('forum_post')." WHERE $where AND position!=1");
			$divide_keys['reply_length'] += floatval($rs['reply_length']);
			//评分值
			$rs = DB::fetch_first("SELECT 
				AVG(rate/ratetimes) AS thread_rate  
				FROM ".DB::table('forum_post')." WHERE $where");
			$divide_keys['thread_rate'] += floatval($rs['thread_rate']);
		}
		$value = array_merge($divide_keys, $value);
		
		//送出鲜花数,鸡蛋数,收到鲜花数,鸡蛋数
		$rs = DB::fetch_first("SELECT AVG(num) AS send_flower_count FROM ".DB::table("common_plugin_fegglog")." WHERE fromuid='$value[uid]' AND `type`=0");
		$send_flower_count = set_empty_zero($rs['send_flower_count']);
		$rs = DB::fetch_first("SELECT AVG(num) AS send_egg_count FROM ".DB::table("common_plugin_fegglog")." WHERE fromuid='$value[uid]' AND `type`=1");
		$send_egg_count = set_empty_zero($rs['send_egg_count']);
		$rs = DB::fetch_first("SELECT AVG(num) AS receive_flower_count FROM ".DB::table("common_plugin_fegglog")." WHERE touid='$value[uid]' AND `type`=0");
		$receive_flower_count = set_empty_zero($rs['receive_flower_count']);
		$rs = DB::fetch_first("SELECT AVG(num) AS receive_egg_count FROM ".DB::table("common_plugin_fegglog")." WHERE touid='$value[uid]' AND `type`=1"); 
		$receive_egg_count = set_empty_zero($rs['receive_egg_count']);
		$value = array_merge(array(
			'send_flower_count' => $send_flower_count, 
			'send_egg_count' => $send_egg_count, 
			'receive_flower_count' => $receive_flower_count, 
			'receive_egg_count' => $receive_egg_count
		), $value);
		
		if($j == 0) {
			$keyArr = array_keys($value);
			$key_name_list = "";
			foreach($keyArr as $key_name) {
				$key_name_list .= "\t".$key_name;
			}
			$key_name_list = substr($key_name_list, 1);
			fwrite($ftitle, $key_name_list);
			fclose($ftitle);
		}
		
		
		
		$data_list = "";
		foreach($value as $data) {
			$data_list .= "\t".$data;
		}
		//将和特征向量无关的uid,avatarstatus,regdate,groupid,adminid,medals,regip,lastip,lastvisit,lastactivity,lastpost,profileprogress放到最后
		
		$data_list = substr($data_list, 1)."\n";
		fwrite($fdata, $data_list);
		if($j < 5) {
			$arr[] = $value;
		}
		$j++;
	}
	fclose($fdata);
	
	echo '<pre>';
	print_r($arr);
	$time_end = time();
	$time_duration = round(($time_end - $time_start)/60,2);
	echo "Records: ".$j."<br/>";
	echo "Time duration: ".$time_duration."min<br/>";
}
function set_empty_zero($v) {
	return $v == "" ? '0' : $v; 
}
function clean_ip($ip) {
	if($ip == 'hidden' || $ip == 'Manual Acting') {
		return "";
	}
}
function clean_extcredits($extcredits) {
	if($extcredits < 0) {
		$extcredits = 0;
	}
	return $extcredits;
}
function clean_medals($medals) {
	$medalsArr = split("/\s+/", $medals);
	$str = "";
	foreach($medalsArr as $v) {
		$arr = explode("|", $v);
		$str .= ",".$arr[0];
	}
	$str = substr($str, 1);
	return $str;
}
?>