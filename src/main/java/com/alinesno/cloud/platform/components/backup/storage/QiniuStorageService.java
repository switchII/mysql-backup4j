package com.alinesno.cloud.platform.components.backup.storage;

import java.io.File;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.qiniu.common.QiniuException;
import com.qiniu.common.Zone;
import com.qiniu.http.Response;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.UploadManager;
import com.qiniu.storage.model.DefaultPutRet;
import com.qiniu.util.Auth;
import com.qiniu.util.StringUtils;


/**
 * 七牛云上傳接口
 * 
 * @author LuoAnDong 20201年7月21日 06:43:10
 */
@SuppressWarnings("deprecation")
public class QiniuStorageService {
	
	private static final Logger logger = LoggerFactory.getLogger(QiniuStorageService.class) ; 

	// 设置需要操作的账号的AK和SK
	private String accessKey;
	private String secretKey;
	private String bucket;
	private String zone = "2";
    private File attachments;
    private String rule ; // 按日期上傳

	public String getRule() {
		return rule;
	}

	public QiniuStorageService setRule(String rule) {
		this.rule = rule;
        return  this;
	}

	public File getAttachments() {
		return attachments;
	}

	public QiniuStorageService setAttachments(File attachments) {
		this.attachments = attachments;
        return  this;
	}

	public String getAccessKey() {
		return accessKey;
	}

	public QiniuStorageService setAccessKey(String accessKey) {
		this.accessKey = accessKey;
        return  this;
	}

	public String getSecretKey() {
		return secretKey;
	}

	public QiniuStorageService setSecretKey(String secretKey) {
		this.secretKey = secretKey;
        return  this;
	}

	public String getBucket() {
		return bucket;
	}

	public QiniuStorageService setBucket(String bucket) {
		this.bucket = bucket;
        return  this;
	}

	public String getZone() {
		return zone;
	}

	public QiniuStorageService setZone(String zone) {
		this.zone = zone;
        return  this;
	}

	public static QiniuStorageService builder() {
		return null;
	}

	/**
	 * 发送文件上传
	 * @return
	 */
	public boolean sendFile() {
		
		Configuration cfg = new Configuration(getZone(zone));
		UploadManager uploadManager = new UploadManager(cfg);

		Auth auth = Auth.create(accessKey, secretKey) ;
		String key = UUID.randomUUID().toString() ; 
		String upToken = auth.uploadToken(bucket);

		try {
			Response response = uploadManager.put(this.getAttachments(), key, upToken);
			DefaultPutRet putRet = new Gson().fromJson(response.bodyString(), DefaultPutRet.class);

			logger.debug("put key = {} , hash = {}" , putRet.key , putRet.hash);

		} catch (QiniuException ex) {
			Response r = ex.response;
			try {
				logger.debug(r.bodyString());
			} catch (QiniuException ex2) {
				logger.error("上传文件错误:",ex2);
			}
			
			return false ; 
		}
		
		return true ; 
	}
	
	private  Zone getZone(String zone) {
		Zone z = Zone.zoneNa0() ;  //北美
		if(StringUtils.isNullOrEmpty(zone)) {
			if(zone.equals("0")) {
				z = Zone.zone0() ;
			}else  if(zone.equals("1")) {
				z = Zone.zone1() ;
			}else  if(zone.equals("2")) {
				z = Zone.zone2() ;
			}
		}
		return z ;
	}

}
