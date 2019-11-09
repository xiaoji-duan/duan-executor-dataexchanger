package com.xiaoji.duan.sas;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import io.vertx.amqpbridge.AmqpBridge;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.FindOptions;
import io.vertx.ext.mongo.MongoClient;

public class MainVerticle extends AbstractVerticle {

	private MongoClient mongodb = null;
	private AmqpBridge bridge = null;

	@Override
	public void start(Promise<Void> startPromise) throws Exception {

		JsonObject config = new JsonObject();
		config.put("host", "mongodb");
		config.put("port", 27017);
		config.put("keepAlive", true);
		config.put("maxWaitQueueSize", 3000);
		mongodb = MongoClient.createShared(vertx, config);

		bridge = AmqpBridge.create(vertx);

		bridge.endHandler(handler -> {
			connectStompServer();
		});

		connectStompServer();
	}
	
	private void subscribeTrigger(String trigger) {
		MessageConsumer<JsonObject> consumer = bridge.createConsumer(trigger);
		if (config().getBoolean("log.info", Boolean.FALSE)) {
			System.out.println("Consumer " + trigger + " subscribed.");
		}
		consumer.handler(vertxMsg -> this.process(trigger, vertxMsg));
	}

	private void process(String consumer, Message<JsonObject> received) {
		if (config().getBoolean("log.info", Boolean.FALSE)) {
			System.out.println("Consumer " + consumer + " received [" + received.body().encode() + "]");
		}

		JsonObject data = received.body().getJsonObject("body");

		String type = data.getJsonObject("context").getString("type", "");
		JsonObject header = data.getJsonObject("context").getJsonObject("header", new JsonObject());
		String datatype = data.getJsonObject("context").getString("datatype", "");		// 拉取数据类型
		String from = data.getJsonObject("context").getString("from", "");		// 请求用户手机号
		String to = data.getJsonObject("context").getString("to", "");			// 复制用户手机号
		JsonObject copyto = data.getJsonObject("context").getJsonObject("copyto", new JsonObject());
		JsonArray request = data.getJsonObject("context").getJsonArray("data", new JsonArray());
		String next = data.getJsonObject("context").getString("next");

		// 客户端上传数据
		if ("push".equals(type)) {
			push(consumer, from, header, request, next);
		}
		
		// 客户端拉取数据请求
		if ("pull".equals(type)) {
			pull(consumer, from, header, datatype, request, next);
		}

		// 复制数据到用户设备
		if ("copy".equals(type)) {
			copy(consumer, from, to, copyto, header, request, next);
		}
		
		// 获取最新数据详情
		if ("fetch".equals(type)) {
			fetch(consumer, datatype, request, next);
		}
	}
	
	private void fetch(String consumer, String datatype, JsonArray datas, String next) {
		String collection = "sas_" + datatype.toLowerCase();

		JsonObject identifies = new JsonObject();
		
		Iterator dataits = datas.iterator();
		
		JsonArray filters = new JsonArray();
		while(dataits.hasNext()) {
			JsonObject data = (JsonObject) dataits.next();
			
			String phoneno = data.getString("phoneno");
			String dataid = data.getString("id");
			
			if (StringUtils.isEmpty(phoneno) || StringUtils.isEmpty(dataid)) {
				continue;
			}
			
			String extendid = phoneno + "_" + dataid;
			
			filters.add(new JsonObject().put("_id", extendid));
		}
		
		if (filters.size() > 0) {	// 存在查询条件
			
			identifies.put("$or", filters);
			
			mongodb.find(collection, identifies, find -> {
				if (find.succeeded()) {
					List<JsonObject> results = find.result();
					
					JsonArray outputs = new JsonArray();
					
					for (JsonObject data : results) {
						// 转换数据格式
						JsonObject converted = new JsonObject();
						
						converted.put("exchangeno", data.getString("_exchangephoneno"));
						converted.put("src", data.getString("_datasrc"));
						converted.put("id", data.getString("_dataid"));
						converted.put("type", data.getString("_datatype"));
						converted.put("title", data.getString("_datatitle"));
						converted.put("datetime", data.getString("_datadatetime"));
						converted.put("main", data.getBoolean("_datamain"));
						converted.put("group", data.getString("_datagroup"));
						converted.put("to", data.getJsonArray("_sharemembers"));
						converted.put("sharestate", data.getJsonObject("_sharestate", new JsonObject()));
						converted.put("security", data.getString("_sharemethod"));
						converted.put("todostate", data.getString("_todostate"));
						converted.put("status", data.getString("_datastate"));
						converted.put("timestamp", data.getLong("_clienttimestamp"));
						converted.put("payload", data.getJsonObject("payload"));
	
						outputs.add(converted);
					}
					
					JsonObject nextctx = new JsonObject().put("context", new JsonObject()
							.put("datas", outputs));
					
					MessageProducer<JsonObject> producer = bridge.createProducer(next);
					producer.send(new JsonObject().put("body", nextctx));
					producer.end();

					System.out.println(
							"Consumer " + consumer + " send to [" + next + "] result [" + nextctx.encode() + "]");
				} else {	// 查询失败
					JsonObject nextctx = new JsonObject().put("context", new JsonObject()
							.put("datas", new JsonArray()));
					
					MessageProducer<JsonObject> producer = bridge.createProducer(next);
					producer.send(new JsonObject().put("body", nextctx));
					producer.end();

					if (config().getBoolean("log.info", Boolean.FALSE)) {
						System.out.println(
								"Consumer " + consumer + " send to [" + next + "] result [" + nextctx.encode() + "]");
					}
				}
			});
			
		} else {	// 没有符合要求的检索条件
			
			JsonObject nextctx = new JsonObject().put("context", new JsonObject()
					.put("datas", new JsonArray()));
			
			MessageProducer<JsonObject> producer = bridge.createProducer(next);
			producer.send(new JsonObject().put("body", nextctx));
			producer.end();

			if (config().getBoolean("log.info", Boolean.FALSE)) {
				System.out.println(
						"Consumer " + consumer + " send to [" + next + "] result [" + nextctx.encode() + "]");
			}
		}
		
	}
	
	private void pushone(Future<JsonObject> endpointFuture, String from, String account, String device, JsonObject next) {
		String src = next.getString("src", account);
		String id = next.getString("id", "Unkown");
		Long timestamp = next.getLong("timestamp", 0L);
		String type = next.getString("type", "Unkown");
		String title = next.getString("title", "");
		String datetime = next.getString("datetime", "");
		Boolean main = next.getBoolean("main", Boolean.FALSE);
		String group = next.getString("group", "");				// 2019/11/07 增加数据分组,用户减少参与人数据同步量
		JsonArray to = next.getJsonArray("to", new JsonArray());
		String security = next.getString("security", "Unkown");
		String status = next.getString("status", "Unkown");
		String invitestate = next.getString("invitestate", "none");
		String todostate = next.getString("todostate", "none");
		JsonObject fields = next.getJsonObject("fields", new JsonObject());
		JsonObject payload = next.getJsonObject("payload");
	
		if (config().getBoolean("log.debug", Boolean.FALSE)) {
			System.out.println("DEBUG [" + type + "][" + src + "][" + id + "] " + from + " => " + to.toString());
		}

		String primaryid = src + "_" + id;		// 用于查询交换区元数据
		String extendid = from + "_" + id;			// 用于存储交换区数据

		// 交换区元数据查询
		JsonObject identify = new JsonObject();
		identify.put("_primaryid", primaryid);

		String collection = "sas_" + type.toLowerCase();
		
		// 交换区既存数据查询
		mongodb.findOne(collection, identify, new JsonObject(), find -> {
			if (find.succeeded()) {
				JsonObject existed = find.result();
				
				Boolean selfpush = Boolean.FALSE;			// 是否本人数据更新
				
				// 交换区数据必须指定来源
				if ("".equals(src) || account.equals(src)) {
					selfpush = Boolean.TRUE;
				}
				
				// 判断是否是发起人删除
				Boolean hasOwnerRemoved = Boolean.FALSE;
				// 判断是否修改了需要共享的字段
				Boolean hasChangedCompared = Boolean.TRUE;
				// 判断是否修改了接受/拒绝
				Boolean hasChangedInviteState = Boolean.FALSE;
				
				Boolean hasCloudData = false;
				
				if (existed != null && !existed.isEmpty()) {
					hasCloudData = true;
				}
				
				JsonObject storage = null;

				// 更新或保存元数据
				if (selfpush) {
					if ("del".equals(status)) {
						hasOwnerRemoved = Boolean.TRUE;
					}

					// 本人更新
					if (hasCloudData) {
						// 存在元数据
						storage = existed.copy();
						if (config().getBoolean("log.info", Boolean.FALSE)) {
							System.out.println("Sender pushed with storaged data.");
						}

						storage.put("_operation", "owner_update");				// 本数据操作
						storage.put("_deviceid", device);
						storage.put("_datatitle", title);
						storage.put("_datadatetime", datetime);
						storage.put("_datamain", main);
						storage.put("_datagroup", group);
						storage.put("_datastate", status);
						storage.put("_invitestate", invitestate);
						storage.put("_todostate", todostate);
						storage.put("_sharestate", mergeShareStatement(storage.getJsonObject("_sharestate"), from, status, invitestate, todostate));
						storage.put("_sharemembers", to);
						storage.put("_sharemethod", security);
						storage.put("_sharefields", fields);
						storage.put("_lastupdate", from);
						storage.put("_clienttimestamp", timestamp);
						storage.put("_servertimestamp", System.currentTimeMillis());
						storage.put("payload", mergePayload(storage.getJsonObject("payload"), payload, fields, ExchangeMethod.OwnerToOwner));
						
						hasChangedCompared = compareChangedPayload(storage.getJsonObject("payload"), payload, fields);
						hasChangedInviteState = compareShareStatement(storage.getJsonObject("_sharestate", new JsonObject()), from, status, invitestate, todostate, "invitestate");
					} else {
						// 不存在元数据
						storage = new JsonObject();
						
						storage.put("_id", extendid);
						storage.put("_operation", "new");				// 本数据操作
						storage.put("_exchangephoneno", from);
						storage.put("_primaryid", primaryid);
						storage.put("_accountid", account);
						storage.put("_deviceid", device);
						storage.put("_phoneno", from);
						storage.put("_datatype", type);
						storage.put("_dataid", id);
						storage.put("_datatitle", title);
						storage.put("_datadatetime", datetime);
						storage.put("_datamain", main);
						storage.put("_datagroup", group);
						storage.put("_datasrc", src);
						storage.put("_datastate", status);
						storage.put("_invitestate", invitestate);
						storage.put("_todostate", todostate);
						storage.put("_sharestate", mergeShareStatement(new JsonObject(), from, status, invitestate, todostate));
						storage.put("_sharemembers", to);
						storage.put("_sharemethod", security);
						storage.put("_sharefields", fields);
						storage.put("_lastupdate", from);
						storage.put("_clienttimestamp", timestamp);
						storage.put("_servertimestamp", System.currentTimeMillis());
						storage.put("payload", payload);

					}
				} else {
					// 他人更新
					if (hasCloudData) {
						// 存在元数据
						storage = existed.copy();
						if (config().getBoolean("log.info", Boolean.FALSE)) {
							System.out.println("Member pushed with sender storaged data.");
						}
						
						// 还原元数据的参与人
						JsonArray reverseto = to.copy();
						reverseto.add(from);
						reverseto.remove(storage.getString("_phoneno", ""));
						
						storage.put("_operation", "other_update");				// 本数据操作
						storage.put("_datatitle", title);
						storage.put("_datadatetime", datetime);
						storage.put("_datamain", main);
						storage.put("_datagroup", group);						// 理论上参与人不能修改重复选项,不会改变数据分组,不需要更新
						storage.put("_sharestate", mergeShareStatement(storage.getJsonObject("_sharestate"), from, status, invitestate, todostate));
						storage.put("_sharemembers", reverseto);
						storage.put("_sharemethod", security);
						storage.put("_lastupdate", from);
						storage.put("_clienttimestamp", timestamp);
						storage.put("_servertimestamp", System.currentTimeMillis());
						storage.put("payload", mergePayload(storage.getJsonObject("payload"), payload, fields, ExchangeMethod.MemberToOwner));

						hasChangedCompared = compareChangedPayload(storage.getJsonObject("payload"), payload, fields);
						hasChangedInviteState = compareShareStatement(storage.getJsonObject("_sharestate", new JsonObject()), from, status, invitestate, todostate, "invitestate");
					} else {
						// 不存在元数据
						// 忽略
					}
				}
				
				// 不存在异常push, 正常更新交换区
				if (storage != null) {
					// 如果存在参与人, 保存/更新共享数据
					// 比较元数据参与人变化
					JsonArray beforemembers = new JsonArray();
					JsonArray aftermembers = storage.getJsonArray("_sharemembers", new JsonArray());

					if (hasCloudData) {
						// 已存在数据
						beforemembers = existed.getJsonArray("_sharemembers", new JsonArray());
					}
					
					JsonObject changed = compare(beforemembers, aftermembers);
					if (config().getBoolean("log.debug", Boolean.FALSE)) {
						System.out.println(changed.encodePrettily());
					}
					
					JsonArray added = changed.getJsonArray("added", new JsonArray());
					JsonArray updated = changed.getJsonArray("updated", new JsonArray());
					JsonArray removed = changed.getJsonArray("removed", new JsonArray());

					// 判断是否改变了参与人
					Boolean hasMemberChanged = Boolean.FALSE;
					
					if (added.size() > 0 || removed.size() > 0) {
						hasMemberChanged = Boolean.TRUE;
					}
					
					JsonArray forwards = new JsonArray();
					String datasrc = storage.getString("_accountid", "Unknown");
					
					// 增加共享人员交换区
					for (String addto : (List<String>) added.getList()) {
						JsonObject todata = storage.copy();

						JsonArray members = aftermembers.copy();
						members.add(todata.getString("_phoneno", ""));	// 增加发起人
						members.remove(addto);							// 移除接收人

						todata.put("_id", addto + "_" + todata.getString("_dataid"));
						todata.put("_exchangephoneno", addto);			// 交换区数据归属人手机号
						todata.put("_datasrc", datasrc);				// 设置数据来源
						todata.put("_sharemembers", members);			// 设置接收人
						todata.put("_operation", "add");				// 本数据操作

						ExchangeMethod exchange = null;
						if (selfpush) {
							if (from.equals(addto)) {
								exchange = ExchangeMethod.OwnerToOwner;	// 这种情况不存在,除非客户端没有过滤参与人是自己
							} else {
								exchange = ExchangeMethod.OwnerToMember;
							}
						} else {
							if (from.equals(addto)) {
								exchange = ExchangeMethod.MemberToSelf;
							} else {
								exchange = ExchangeMethod.MemberToMember;
							}
						}
						todata.put("payload", mergePayload(storage.getJsonObject("payload"), payload, fields, exchange));
						
						if (config().getBoolean("log.debug", Boolean.FALSE)) {
							System.out.println("DEBUG [" + type + "][" + src + "][" + id + "] " + from + " add " + addto);
						}

						forwards.add(todata);
					}

					// 共享人员更新交换区
					for (String updateto : (List<String>) updated.getList()) {
						// 控制非共向字段修改减少他人同步处理
						if (!hasMemberChanged && !hasChangedCompared && !hasOwnerRemoved && !hasChangedInviteState) {
							break;
						}

						JsonObject todata = storage.copy();

						JsonArray members = aftermembers.copy();
						members.add(todata.getString("_phoneno", ""));	// 增加发起人
						members.remove(updateto);						// 移除接收人

						todata.put("_id", updateto + "_" + todata.getString("_dataid"));
						todata.put("_exchangephoneno", updateto);		// 交换区数据归属人手机号
						todata.put("_datasrc", datasrc);				// 设置数据来源
						todata.put("_sharemembers", members);			// 设置接收人
						todata.put("_operation", "update");				// 本数据操作

						ExchangeMethod exchange = null;
						if (selfpush) {
							if (from.equals(updateto)) {
								exchange = ExchangeMethod.OwnerToOwner;	// 这种情况不存在,除非客户端没有过滤参与人是自己
							} else {
								exchange = ExchangeMethod.OwnerToMember;
							}
						} else {
							if (from.equals(updateto)) {
								exchange = ExchangeMethod.MemberToSelf;
							} else {
								exchange = ExchangeMethod.MemberToMember;
							}
						}
						todata.put("payload", mergePayload(storage.getJsonObject("payload"), payload, fields, exchange));
						
						if (config().getBoolean("log.debug", Boolean.FALSE)) {
							System.out.println("DEBUG [" + type + "][" + src + "][" + id + "] " + from + " update " + updateto);
						}

						forwards.add(todata);
					}

					// 删除共享人员交换区
					for (String removeto : (List<String>) removed.getList()) {
						JsonObject todata = storage.copy();

						JsonArray members = aftermembers.copy();
						members.add(todata.getString("_phoneno", ""));	// 增加发起人
						members.remove(removeto);						// 移除接收人

						todata.put("_id", removeto + "_" + todata.getString("_dataid"));
						todata.put("_exchangephoneno", removeto);		// 交换区数据归属人手机号
						todata.put("_datasrc", datasrc);				// 设置数据来源
						todata.put("_datastate", "del");				// 标记为删除
						todata.put("_sharemembers", members);			// 设置接收人
						todata.put("_operation", "remove");				// 本数据操作

						ExchangeMethod exchange = null;
						if (selfpush) {
							if (from.equals(removeto)) {
								exchange = ExchangeMethod.OwnerToOwner;	// 这种情况不存在,除非客户端没有过滤参与人是自己
							} else {
								exchange = ExchangeMethod.OwnerToMember;
							}
						} else {
							if (from.equals(removeto)) {
								exchange = ExchangeMethod.MemberToSelf;
							} else {
								exchange = ExchangeMethod.MemberToMember;
							}
						}
						todata.put("payload", mergePayload(storage.getJsonObject("payload"), payload, fields, exchange));
						
						if (config().getBoolean("log.debug", Boolean.FALSE)) {
							System.out.println("DEBUG [" + type + "][" + src + "][" + id + "] " + from + " remove " + removeto);
						}

						forwards.add(todata);
					}
					
					// 保存元数据
					savepush(endpointFuture, collection, storage, fields, forwards);
				} else {
					// 异常push
					if (config().getBoolean("log.error", Boolean.TRUE)) {
						System.out.println("异常push [" + type + "][" + id + "] "+ from + " => " + to);
					}

					endpointFuture.complete(new JsonObject().put("datas", new JsonArray()));
				}

			} else {
				if (config().getBoolean("log.error", Boolean.TRUE)) {
					System.out.println("数据库查询失败 [" + type + "][" + id + "] "+ from + " => " + to);
				}

				find.cause().printStackTrace();
				
				// 元数据查询失败
				endpointFuture.complete(new JsonObject().put("datas", new JsonArray()));
			}
		});
	}
	
	private void savepush(Future<JsonObject> endpointFuture, String collection, JsonObject storage, JsonObject fields, JsonArray forwards) {

		mongodb.save(collection, storage, saved -> {
			if (saved.succeeded()) {
				List<Future<JsonObject>> compositeFutures = new LinkedList<>();

				Future<JsonObject> savedsrcFuture = Future.future();
				compositeFutures.add(savedsrcFuture);
				
				// 更新/保存成功
				Iterator<Object> forwardors = forwards.iterator();
				while(forwardors.hasNext()) {
					Future<JsonObject> savedFuture = Future.future();
					compositeFutures.add(savedFuture);

					JsonObject forward = (JsonObject) forwardors.next();
					String currentmember = forward.getString("_exchangephoneno", "");	// 当前数据所属成员手机
					Boolean main = forward.getBoolean("_datamain", Boolean.FALSE);		// 当前数据是否为主数据
					String lastupdate = forward.getString("_lastupdate");				// 最新更新人手机

					mongodb.findOne(collection, new JsonObject().put("_id", forward.getString("_id")), new JsonObject(), find -> {
						if (find.succeeded()) {
							JsonObject existed = find.result();
							Boolean ignore = Boolean.FALSE;
							
							// 存在
							if (existed != null && !existed.isEmpty()) {
								if (currentmember.equals(lastupdate)) {
									forward.put("payload", mergePayload(existed.getJsonObject("payload"), forward.getJsonObject("payload"), fields, ExchangeMethod.MemberToSelf));
								} else {
									forward.put("payload", mergePayload(existed.getJsonObject("payload"), forward.getJsonObject("payload"), fields, ExchangeMethod.OwnerToMember));
								}
							} else {
								// 第一次存储,去除不共享字段
								// 第一次存储,如果不是主数据,则不存储
								if (main) {
									forward.put("payload", mergePayload(new JsonObject(), forward.getJsonObject("payload"), fields, ExchangeMethod.OwnerToMember));
								} else {
									ignore = Boolean.TRUE;
								}
							}
							
							// 保存共享数据
							if (!ignore) {
								mongodb.save(collection, forward, forwardsaved -> {
									if (forwardsaved.succeeded()) {
										savedFuture.complete(forward);
									} else {
										// 共享数据保存失败
										savedFuture.complete(new JsonObject());
									}
								});
							} else {
								savedFuture.complete(new JsonObject());
							}
						} else {	// 访问失败
							savedFuture.complete(new JsonObject());
							// 保存共享数据
//							mongodb.save(collection, forward, forwardsaved -> {
//								if (forwardsaved.succeeded()) {
//									savedFuture.complete(forward);
//								} else {
//									// 共享数据保存失败
//									savedFuture.complete(new JsonObject());
//								}
//							});
						}
					});
				}
				
				savedsrcFuture.complete(storage);
				
				CompositeFuture.all(Arrays.asList(compositeFutures.toArray(new Future[compositeFutures.size()])))
				.map(v -> compositeFutures.stream().map(Future::result).collect(Collectors.toList()))
				.setHandler(handler -> {
					if (handler.succeeded()) {
						List<JsonObject> results = handler.result();
						
						JsonArray datas = new JsonArray();
						for (JsonObject result : results) {
							if (result != null && !result.isEmpty())
								datas.add(result);
						}

						endpointFuture.complete(new JsonObject().put("datas", datas));
					} else {
						endpointFuture.complete(new JsonObject().put("datas", new JsonArray()));
					}
				});
			} else {
				// 元数据保存失败
				endpointFuture.complete(new JsonObject().put("datas", new JsonArray()));
			}
		});
	}
	
	private void push(String consumer, String from, JsonObject header, JsonArray data, String nextTask) {
		String account = header.getString("ai");
		String device = header.getString("di");
		
		List<Future<JsonObject>> compositeFutures = new LinkedList<>();

		Iterator<Object> iterator = data.iterator();
		while(iterator.hasNext()) {
			JsonObject next = (JsonObject) iterator.next();
			
			Future<JsonObject> endpointFuture = Future.future();
			compositeFutures.add(endpointFuture);

			pushone(endpointFuture, from, account, device, next);
		}
		
		CompositeFuture.all(Arrays.asList(compositeFutures.toArray(new Future[compositeFutures.size()])))
		.map(v -> compositeFutures.stream().map(Future::result).collect(Collectors.toList()))
		.setHandler(handler -> {
			if (handler.succeeded()) {
				List<JsonObject> results = handler.result();
				
				JsonArray datas = new JsonArray();
				
				// 根据手机号返回数据
				Map<String, JsonArray> databyphone = new LinkedHashMap<String, JsonArray>();
				for (JsonObject result : results) {
					JsonArray subdatas = result.getJsonArray("datas", new JsonArray());
					
					Iterator<Object> it = subdatas.iterator();
					while(it.hasNext()) {
						JsonObject next = (JsonObject) it.next();
						
						String phoneno = next.getString("_exchangephoneno", "");
						String type = next.getString("_datatype", "");
						String src = next.getString("_datasrc", "");
						String id = next.getString("_dataid", "");

						if (config().getBoolean("log.debug", Boolean.FALSE)) {
							System.out.println("DEBUG [" + type + "][" + src + "][" + id + "] " + from + " => " + phoneno);
						}

						if (!"".equals(phoneno)) {
							JsonArray phonedatas = (JsonArray) databyphone.get(phoneno);
							
							if (phonedatas == null) {
								phonedatas = new JsonArray();
							}
							
							phonedatas.add(next);
							
							databyphone.put(phoneno, phonedatas);
						}
					}
				}
				
				for (String key : databyphone.keySet()) {
					datas.add(new JsonObject()
							.put("_exchangephoneno", key)
							.put("datas", databyphone.get(key)));
				}
				
				// 如果数据超过10条，需要分割处理
//				if (datas != null && datas.size() > 5) {
					Iterator<Object> itdata = datas.iterator();
					JsonArray subdatas = new JsonArray();
					while (itdata.hasNext()) {
						
						subdatas.add((JsonObject) itdata.next());

						JsonObject nextctx = new JsonObject().put("more", itdata.hasNext()).put("context", new JsonObject()
								.put("from", from)
								.put("header", header)
								.put("datas", subdatas));
						
						MessageProducer<JsonObject> producer = bridge.createProducer(nextTask);
						producer.send(new JsonObject().put("body", nextctx));
						producer.end();
		
						if (config().getBoolean("log.info", Boolean.FALSE)) {
							System.out.println(
									"Consumer " + consumer + " send to [" + nextTask + "] result [" + nextctx.encode() + "]");
						}
	
						subdatas = new JsonArray();
					}
					
//					if (subdatas.size() > 0) {
//						JsonObject nextctx = new JsonObject().put("more", Boolean.FALSE).put("context", new JsonObject()
//								.put("from", from)
//								.put("header", header)
//								.put("datas", subdatas));
//						
//						MessageProducer<JsonObject> producer = bridge.createProducer(nextTask);
//						producer.send(new JsonObject().put("body", nextctx));
//						producer.end();
//		
//						if (config().getBoolean("log.info", Boolean.FALSE)) {
//							System.out.println(
//									"Consumer " + consumer + " send to [" + nextTask + "] result [" + nextctx.encode() + "]");
//						}
//					}
//				} else {
//					JsonObject nextctx = new JsonObject().put("more", Boolean.FALSE).put("context", new JsonObject()
//							.put("from", from)
//							.put("header", header)
//							.put("datas", datas));
//					
//					MessageProducer<JsonObject> producer = bridge.createProducer(nextTask);
//					producer.send(new JsonObject().put("body", nextctx));
//					producer.end();
//
//					if (config().getBoolean("log.info", Boolean.FALSE)) {
//						System.out.println(
//								"Consumer " + consumer + " send to [" + nextTask + "] result [" + nextctx.encode() + "]");
//					}
//				}
			} else {
				handler.cause().printStackTrace();
				
				JsonObject nextctx = new JsonObject().put("context", new JsonObject()
						.put("from", from)
						.put("header", header)
						.put("datas", new JsonArray()));
				
				MessageProducer<JsonObject> producer = bridge.createProducer(nextTask);
				producer.send(new JsonObject().put("body", nextctx));
				producer.end();

				if (config().getBoolean("log.info", Boolean.TRUE)) {
					System.out.println(
							"Consumer " + consumer + " send to [" + nextTask + "] result [" + nextctx.encode() + "]");
				}
			}
		});
	}
	
	private void copyonedevice(Future<JsonObject> endpointFuture, String to, String deviceid, String id, String type, JsonObject data, boolean force) {
		String partition = to.substring(to.length() - 1);	// 根据帐户ID尾号分区存储
		String collection = "sas_" + partition + "_" + type.toLowerCase();
		
		String devicestorageid = to + "_" + deviceid + "_" + id;
		
		// 如果是默认设备,直接保存,非默认设备,如果该设备存在该数据则保存,否则不保存
		data.put("_id", devicestorageid);
		if (force) {
			mongodb.save(collection, data, saved -> {
				if (saved.succeeded()) {
					String src = data.getString("_datasrc", "");
					
					if (config().getBoolean("log.debug", Boolean.FALSE)) {
						System.out.println("DEBUG [" + type + "][" + src + "][" + id + "] " + to + " => " + deviceid);
					}

					// 转换数据格式
					JsonObject converted = new JsonObject();
					
					converted.put("src", data.getString("_datasrc"));
					converted.put("id", data.getString("_dataid"));
					converted.put("type", data.getString("_datatype"));
					converted.put("title", data.getString("_datatitle"));
					converted.put("datetime", data.getString("_datadatetime"));
					converted.put("main", data.getBoolean("_datamain"));
					converted.put("group", data.getString("_datagroup"));
					converted.put("operation", data.getString("_operation"));
					converted.put("to", data.getJsonArray("_sharemembers"));
					converted.put("todevice", deviceid);
					converted.put("sharestate", data.getJsonObject("_sharestate", new JsonObject()));
					converted.put("security", data.getString("_sharemethod"));
					converted.put("todostate", data.getString("_todostate"));
					converted.put("status", data.getString("_datastate"));
					converted.put("timestamp", data.getLong("_clienttimestamp"));
					converted.put("payload", data.getJsonObject("payload"));
					
					endpointFuture.complete(converted);
				} else {
					// 保存失败
					endpointFuture.complete(new JsonObject());
				}
			});
		} else {
			JsonObject identify = new JsonObject();
			identify.put("_id", devicestorageid);
			
			mongodb.findOne(collection, identify, new JsonObject(), find -> {
				if (find.succeeded()) {
					JsonObject existed = find.result();
					
					if (existed != null && !existed.isEmpty()) {
						mongodb.save(collection, data, saved -> {
							if (saved.succeeded()) {
								String src = data.getString("_datasrc", "");
								
								if (config().getBoolean("log.debug", Boolean.FALSE)) {
									System.out.println("DEBUG [" + type + "][" + src + "][" + id + "] " + to + " => " + deviceid);
								}

								// 转换数据格式
								JsonObject converted = new JsonObject();
								
								converted.put("src", data.getString("_datasrc"));
								converted.put("id", data.getString("_dataid"));
								converted.put("type", data.getString("_datatype"));
								converted.put("title", data.getString("_datatitle"));
								converted.put("datetime", data.getString("_datadatetime"));
								converted.put("main", data.getBoolean("_datamain"));
								converted.put("group", data.getString("_datagroup"));
								converted.put("operation", data.getString("_operation"));
								converted.put("to", data.getJsonArray("_sharemembers"));
								converted.put("todevice", deviceid);
								converted.put("sharestate", data.getJsonObject("_sharestate", new JsonObject()));
								converted.put("security", data.getString("_sharemethod"));
								converted.put("todostate", data.getString("_todostate"));
								converted.put("status", data.getString("_datastate"));
								converted.put("timestamp", data.getLong("_clienttimestamp"));
								converted.put("payload", data.getJsonObject("payload"));
								
								endpointFuture.complete(converted);
							} else {
								// 保存失败
								endpointFuture.complete(new JsonObject());
							}
						});
					} else {
						// 该设备不存在此数据
						endpointFuture.complete(new JsonObject());
					}
				} else {
					// 该设备数据查询失败
					endpointFuture.complete(new JsonObject());
				}
			});
		}
	}
	
	private void copyone(Future<JsonObject> endpointFuture, Boolean self, String fromdeviceid, String to, JsonObject defaultdevice, JsonArray devices, JsonObject data) {

		List<String> uniquedevices = new ArrayList<String>();
		
		List<Future<JsonObject>> compositeFutures = new LinkedList<>();

		Iterator<Object> iterator = devices.iterator();

		String defaultdeviceid = defaultdevice.getString("uuid", "");
		String dataid = data.getString("_dataid", "Unknown");
		String datatype = data.getString("_datatype", "Unknown");

		while(iterator.hasNext()) {
			JsonObject device = (JsonObject) iterator.next();
			String deviceid = device.getString("uuid", "");
			
			// 重复设备ID或者设备ID为空,忽略
			if ("".equals(deviceid) || uniquedevices.contains(deviceid)) {
				continue;
			}

			uniquedevices.add(deviceid);
			
			Future<JsonObject> deviceFuture = Future.future();
			compositeFutures.add(deviceFuture);

			JsonObject devicedata = data.copy();
			
			// 接收方默认设备或者发送方发送设备强制复制数据
			boolean force = defaultdeviceid.equals(deviceid) || (self && fromdeviceid.equals(deviceid)) || "browser".equals(deviceid);
			
			copyonedevice(deviceFuture, to, deviceid, dataid, datatype, devicedata, force);
		}
		
		CompositeFuture.all(Arrays.asList(compositeFutures.toArray(new Future[compositeFutures.size()])))
		.map(v -> compositeFutures.stream().map(Future::result).collect(Collectors.toList()))
		.setHandler(handler -> {
			if (handler.succeeded()) {
				List<JsonObject> results = handler.result();
				
				JsonArray datas = new JsonArray();
				
				for (JsonObject result : results) {
					if (result == null || result.isEmpty()) {
						continue;
					}
					
					datas.add(result);
				}
				
				endpointFuture.complete(new JsonObject().put("datas", datas));
			} else {
				endpointFuture.complete(new JsonObject().put("datas", new JsonArray()));
			}
		});
	}
	
	private void copy(String consumer, String from, String to, JsonObject copyto, JsonObject header, JsonArray data, String nextTask) {
		
		// 为查询到用户信息,非注册用户,忽略拷贝
		if (copyto == null || copyto.isEmpty() || data == null || data.size() <= 0) {
			JsonObject nextctx = new JsonObject()
					.put("context", new JsonObject()
							.put("from", from)
							.put("to", to)
							.put("copyto", copyto)
							.put("header", header)
							.put("datas", new JsonArray()));
			
			MessageProducer<JsonObject> producer = bridge.createProducer(nextTask);
			producer.send(new JsonObject().put("body", nextctx));
			producer.end();

			if (config().getBoolean("log.info", Boolean.FALSE)) {
				System.out.println(
						"Consumer " + consumer + " send to [" + nextTask + "] result [" + nextctx.encode() + "]");
			}
			
			return;
		}
		
		String toaccountid = copyto.getString("openid", "");
		String tophoneno = copyto.getString("phoneno", "");
		
		Boolean self = from.equals(tophoneno);
		
		String fromdeviceid = header.getString("di", "");
		JsonObject todevice = copyto.getJsonObject("device", new JsonObject());
		JsonArray todevices = copyto.getJsonArray("devices", new JsonArray());
		
		// 不存在默认设备,则设置为浏览器
		if (todevice.isEmpty()) {
			todevice = new JsonObject();
			
			todevice.put("uuid", "browser");
		}
		
		todevices.add(new JsonObject().put("uuid", "browser"));		// 增加浏览器设备
		
		List<Future<JsonObject>> compositeFutures = new LinkedList<>();

		Iterator<Object> iterator = data.iterator();
		while(iterator.hasNext()) {
			JsonObject next = (JsonObject) iterator.next();
			
			Future<JsonObject> endpointFuture = Future.future();
			compositeFutures.add(endpointFuture);

			String type = next.getString("_datatype", "");
			String src = next.getString("_datasrc", "");
			String id = next.getString("_dataid", "");
			
			if (config().getBoolean("log.debug", Boolean.FALSE)) {
				System.out.println("DEBUG [" + type + "][" + src + "][" + id + "] " + from + " copyto " + to + ":" +tophoneno);
			}

			copyone(endpointFuture, self, fromdeviceid, toaccountid, todevice, todevices, next);
		}
		
		CompositeFuture.all(Arrays.asList(compositeFutures.toArray(new Future[compositeFutures.size()])))
		.map(v -> compositeFutures.stream().map(Future::result).collect(Collectors.toList()))
		.setHandler(handler -> {
			if (handler.succeeded()) {
				List<JsonObject> results = handler.result();
				
				JsonArray datas = new JsonArray();
				
				for (JsonObject result : results) {
					datas.addAll(result.getJsonArray("datas", new JsonArray()));
				}
				
				// 如果数据超过10条，需要分割处理
//				if (datas != null && datas.size() > 5) {
					Iterator<Object> itdata = datas.iterator();
					JsonArray subdatas = new JsonArray();
					while (itdata.hasNext()) {
						
						if (subdatas.size() < 5) {
							subdatas.add((JsonObject) itdata.next());
						} else {
							JsonObject nextctx = new JsonObject().put("more", itdata.hasNext()).put("context", new JsonObject()
									.put("from", from)
									.put("to", tophoneno)
									.put("copyto", copyto)
									.put("header", header)
									.put("datas", subdatas));
							
							MessageProducer<JsonObject> producer = bridge.createProducer(nextTask);
							producer.send(new JsonObject().put("body", nextctx));
							producer.end();
			
							if (config().getBoolean("log.info", Boolean.FALSE)) {
								System.out.println(
										"Consumer " + consumer + " send to [" + nextTask + "] result [" + nextctx.encode() + "]");
							}

							subdatas = new JsonArray();
						}
					}
					
					if (subdatas.size() > 0) {
						JsonObject nextctx = new JsonObject().put("more", Boolean.FALSE).put("context", new JsonObject()
								.put("from", from)
								.put("to", tophoneno)
								.put("copyto", copyto)
								.put("header", header)
								.put("datas", subdatas));
						
						MessageProducer<JsonObject> producer = bridge.createProducer(nextTask);
						producer.send(new JsonObject().put("body", nextctx));
						producer.end();
		
						if (config().getBoolean("log.info", Boolean.FALSE)) {
							System.out.println(
									"Consumer " + consumer + " send to [" + nextTask + "] result [" + nextctx.encode() + "]");
						}
					}
//				} else {
//					JsonObject nextctx = new JsonObject().put("context", new JsonObject()
//							.put("from", from)
//							.put("to", to)
//							.put("copyto", copyto)
//							.put("header", header)
//							.put("datas", datas));
//					
//					MessageProducer<JsonObject> producer = bridge.createProducer(nextTask);
//					producer.send(new JsonObject().put("body", nextctx));
//					producer.end();
//	
//					if (config().getBoolean("log.info", Boolean.FALSE)) {
//						System.out.println(
//								"Consumer " + consumer + " send to [" + nextTask + "] result [" + nextctx.encode() + "]");
//					}
//				}
			} else {
				handler.cause().printStackTrace();
				
				JsonObject nextctx = new JsonObject().put("context", new JsonObject()
						.put("from", from)
						.put("to", tophoneno)
						.put("copyto", copyto)
						.put("header", header)
						.put("datas", new JsonArray()));
				
				MessageProducer<JsonObject> producer = bridge.createProducer(nextTask);
				producer.send(new JsonObject().put("body", nextctx));
				producer.end();

				if (config().getBoolean("log.info", Boolean.FALSE)) {
					System.out.println(
							"Consumer " + consumer + " send to [" + nextTask + "] result [" + nextctx.encode() + "]");
				}
			}
		});
	}

	private void pullfull(Future<JsonObject> endpointFuture, String from, String account, String device, String datatype) {
		// 查询本帐号
		String collection = "sas_" + datatype.toLowerCase();	// 本帐号数据
		
		JsonObject condition = new JsonObject();
		condition.put("_exchangephoneno", from)
				 .put("_dataid", 
						new JsonObject().put("$exists", true))
				 .put("_datastate", "undel");
	
		mongodb.find(collection, condition, finddiff -> {
			if (finddiff.succeeded()) {
				List<JsonObject> diff = finddiff.result();
				
				List<Future<JsonObject>> compositeFutures = new LinkedList<>();

				// 复制差分数据到设备数据
				for (JsonObject diffone : diff) {
					String id = diffone.getString("_dataid");

					Future<JsonObject> subFuture = Future.future();
					compositeFutures.add(subFuture);

					copyonedevice(subFuture, from, device, id, datatype, diffone, true);
				}
				
				CompositeFuture.all(Arrays.asList(compositeFutures.toArray(new Future[compositeFutures.size()])))
				.map(v -> compositeFutures.stream().map(Future::result).collect(Collectors.toList()))
				.setHandler(handler -> {
					if (handler.succeeded()) {
						List<JsonObject> results = handler.result();
						
						endpointFuture.complete(new JsonObject().put("datas", results));
					} else {
						// 汇总失败
						endpointFuture.complete(new JsonObject().put("datas", new JsonArray()));
					}
				});
			} else {
				// 查询本帐号失败
				endpointFuture.complete(new JsonObject().put("datas", new JsonArray()));
			}
		});
	}
	
	private void pulldiff(Future<JsonObject> endpointFuture, String from, String account, String device, String datatype) {
		String partition = account.substring(account.length() - 1);	// 根据帐户ID尾号分区存储
		String devicecollection = "sas_" + partition + "_" + datatype.toLowerCase();
		
		FindOptions options = new FindOptions();
		options.setFields(new JsonObject().put("_dataid", true));
		
		// 查询本设备
		mongodb.findWithOptions(devicecollection, new JsonObject(), options, finddevice -> {
			if (finddevice.succeeded()) {
				List<JsonObject> devicedatas = finddevice.result();
				
				JsonArray notin = new JsonArray();
				for (JsonObject data : devicedatas) {
					notin.add(data.getString("_dataid"));
				}
				
				// 查询本帐号
				String collection = "sas_" + datatype.toLowerCase();	// 本帐号数据
				
				JsonObject condition = new JsonObject();
				condition.put("$and", new JsonArray()
						.add(new JsonObject().put("_dataid", 
								new JsonObject().put("$exists", true)))
						.add(new JsonObject().put("_dataid", 
								new JsonObject().put("$not", 
										new JsonObject().put("$in", notin))))
				);
			
				mongodb.find(collection, condition, finddiff -> {
					if (finddiff.succeeded()) {
						List<JsonObject> diff = finddiff.result();
						
						List<Future<JsonObject>> compositeFutures = new LinkedList<>();

						// 复制差分数据到设备数据
						for (JsonObject diffone : diff) {
							String id = diffone.getString("_dataid");

							Future<JsonObject> subFuture = Future.future();
							compositeFutures.add(subFuture);

							copyonedevice(subFuture, from, device, id, datatype, diffone, true);
						}
						
						CompositeFuture.all(Arrays.asList(compositeFutures.toArray(new Future[compositeFutures.size()])))
						.map(v -> compositeFutures.stream().map(Future::result).collect(Collectors.toList()))
						.setHandler(handler -> {
							if (handler.succeeded()) {
								List<JsonObject> results = handler.result();
								
								endpointFuture.complete(new JsonObject().put("datas", results));
							} else {
								// 汇总失败
								endpointFuture.complete(new JsonObject().put("datas", new JsonArray()));
							}
						});
					} else {
						// 查询本帐号失败
						endpointFuture.complete(new JsonObject().put("datas", new JsonArray()));
					}
				});
			} else {
				// 查询本设备失败
				endpointFuture.complete(new JsonObject().put("datas", new JsonArray()));
			}
		});
	}
	
	private void pull(Future<JsonObject> endpointFuture, String account, String device, String datatype, JsonArray data) {
		String partition = account.substring(account.length() - 1);	// 根据帐户ID尾号分区存储
		String collection = "sas_" + partition + "_" + datatype.toLowerCase();
		
		JsonArray or = new JsonArray();

		Iterator<Object> iterator = data.iterator();
		while(iterator.hasNext()) {
			String next = (String) iterator.next();

			or.add(new JsonObject().put("_id", account + "_" + device + "_" + next));
		}

		JsonObject condition = new JsonObject();
		condition.put("$or", or);
		
		mongodb.find(collection, condition, find -> {
			if (find.succeeded()) {
				List<JsonObject> datas = find.result();
				
				if (datas != null && datas.size() > 0) {
					List<JsonObject> outputs = new LinkedList<JsonObject>();
					
					for (JsonObject single : datas) {
						// 转换数据格式
						JsonObject converted = new JsonObject();
						
						converted.put("src", single.getString("_datasrc"));
						converted.put("id", single.getString("_dataid"));
						converted.put("type", single.getString("_datatype"));
						converted.put("title", single.getString("_datatitle"));
						converted.put("datetime", single.getString("_datadatetime"));
						converted.put("main", single.getBoolean("_datamain"));
						converted.put("group", single.getString("_datagroup"));
						converted.put("operation", single.getString("_operation"));
						converted.put("to", single.getJsonArray("_sharemembers"));
						converted.put("sharestate", single.getJsonObject("_sharestate", new JsonObject()));
						converted.put("security", single.getString("_sharemethod"));
						converted.put("todostate", single.getString("_todostate"));
						converted.put("status", single.getString("_datastate"));
						converted.put("timestamp", single.getLong("_clienttimestamp"));
						converted.put("payload", single.getJsonObject("payload"));
						
						outputs.add(converted);
					}
					
					endpointFuture.complete(new JsonObject().put("datas", outputs));
				} else {
					endpointFuture.complete(new JsonObject().put("datas", new ArrayList<JsonObject>()));
				}
			} else {
				endpointFuture.complete(new JsonObject().put("datas", new ArrayList<JsonObject>()));
			}
		});
		
	}
	
	/**
	 * 客户端拉取子数据
	 * 重复日程的子日程,第一次共向德时候不会被复制到参与人交换区中
	 * 参与人接受这个主日程之后，客户端请求拉取所有子日程
	 * 
	 * @param endpointFuture
	 * @param account
	 * @param device
	 * @param datatype
	 * @param data
	 */
	private void pullgroup(Future<JsonObject> endpointFuture, String account, String device, String datatype, JsonArray data) {
		String partition = account.substring(account.length() - 1);	// 根据帐户ID尾号分区存储
		String collection = "sas_" + partition + "_" + datatype.toLowerCase();
		
		JsonArray or = new JsonArray();

		Iterator<Object> iterator = data.iterator();
		while(iterator.hasNext()) {
			String next = (String) iterator.next();

			or.add(new JsonObject().put("_id", account + "_" + device + "_" + next));
		}

		JsonObject condition = new JsonObject();
		condition.put("$or", or);
		
		// 找到主日程
		mongodb.find(collection, condition, find -> {
			if (find.succeeded()) {
				List<JsonObject> datas = find.result();
				
				if (datas != null && datas.size() > 0) {
					
					for (JsonObject single : datas) {
						// 拉取请求发起人设备上的数据
						String phoneno = single.getString("_phoneno");
						String from = single.getString("_exchangephoneno");
						String groupid = single.getString("_dataid");
						
						// 复制子日程到本用户
						String maincollection = "sas_" + datatype.toLowerCase();
						
						JsonObject maincondition = new JsonObject();
						maincondition.put("_exchangephoneno", phoneno);
						maincondition.put("_datagroup", groupid);
						maincondition.put("_datastate", "undel");
						
						mongodb.find(maincollection, maincondition, mainfind -> {
							if (mainfind.succeeded()) {
								List<JsonObject> diff = mainfind.result();
								
								List<Future<JsonObject>> compositeFutures = new LinkedList<>();

								// 复制差分数据到设备数据
								for (JsonObject diffone : diff) {
									String id = diffone.getString("_dataid");
									String datasrc = diffone.getString("_datasrc");
									String type = diffone.getString("_datatype");

									Future<JsonObject> subFuture = Future.future();
									compositeFutures.add(subFuture);

									// 数据转换 发起人数据转换成受邀人数据
									JsonObject todata = diffone.copy();
									
									JsonArray members = todata.getJsonArray("_sharemembers", new JsonArray());
									members.add(todata.getString("_phoneno", ""));	// 增加发起人
									members.remove(phoneno);						// 移除接收人

									todata.put("_id", phoneno + "_" + todata.getString("_dataid"));
									todata.put("_exchangephoneno", phoneno);		// 交换区数据归属人手机号
									todata.put("_datasrc", datasrc);				// 设置数据来源
									todata.put("_sharemembers", members);			// 设置接收人
									todata.put("_operation", "update");				// 本数据操作

									ExchangeMethod exchange = ExchangeMethod.OwnerToMember;

									JsonObject fields = todata.getJsonObject("_sharefields");
									JsonObject payload = todata.getJsonObject("payload");
									todata.put("payload", mergePayload(payload, payload, fields, exchange));
									
									if (config().getBoolean("log.debug", Boolean.FALSE)) {
										System.out.println("DEBUG [" + type + "][" + datasrc + "][" + id + "] " + from + " pull to " + phoneno);
									}
									
									copyonedevice(subFuture, from, device, id, datatype, diffone, true);
								}
								
								CompositeFuture.all(Arrays.asList(compositeFutures.toArray(new Future[compositeFutures.size()])))
								.map(v -> compositeFutures.stream().map(Future::result).collect(Collectors.toList()))
								.setHandler(handler -> {
									if (handler.succeeded()) {
										List<JsonObject> results = handler.result();
										
										endpointFuture.complete(new JsonObject().put("datas", results));
									} else {
										// 汇总失败
										endpointFuture.complete(new JsonObject().put("datas", new JsonArray()));
									}
								});
							} else {
								// 查询本帐号失败
								endpointFuture.complete(new JsonObject().put("datas", new JsonArray()));
							}
						});
						
					}
					
				} else {
					endpointFuture.complete(new JsonObject().put("datas", new JsonArray()));
				}
			} else {
				endpointFuture.complete(new JsonObject().put("datas", new JsonArray()));
			}
		});
	}
	
	private void pull(String consumer, String from, JsonObject header, String datatype, JsonArray data, String nextTask) {
		String account = header.getString("ai");
		String device = header.getString("di");

		List<Future<JsonObject>> compositeFutures = new LinkedList<>();

		if (data.size() > 0) {
			// 拉取指定数据/分组数据
			Future<JsonObject> endpointFuture = Future.future();
			compositeFutures.add(endpointFuture);

			// 拉去分组数据
			if (datatype.contains("#")) {
				String type = datatype.substring(0, datatype.indexOf("#"));
				pullgroup(endpointFuture, account, device, type, data);
			} else {	// 拉去指定数据
				pull(endpointFuture, account, device, datatype, data);
			}
		} else {
			if ("*".equals(datatype)) {
				// 拉取所有数据(本帐号与本设备差分数据和本设备既存数据)
				List<String> types = new ArrayList<String>();
				types.add("Plan");
				types.add("Attachment");
				types.add("PlanItem");
				types.add("Agenda");
				types.add("Task");
				types.add("MiniTask");
				types.add("Memo");
				
				for (String type : types) {
					Future<JsonObject> endpointFuture = Future.future();
					compositeFutures.add(endpointFuture);

					pullfull(endpointFuture, from, account, device, type);
				}
			} else {
				// 拉取所有差分数据(本帐号与本设备差分数据)
				Future<JsonObject> endpointFuture = Future.future();
				compositeFutures.add(endpointFuture);

				pulldiff(endpointFuture, from, account, device, datatype);
			}
		}
		
		CompositeFuture.all(Arrays.asList(compositeFutures.toArray(new Future[compositeFutures.size()])))
		.map(v -> compositeFutures.stream().map(Future::result).collect(Collectors.toList()))
		.setHandler(handler -> {
			if (handler.succeeded()) {
				List<JsonObject> results = handler.result();
				
				JsonArray datas = new JsonArray();
				for (JsonObject result : results) {
					datas.addAll(result.getJsonArray("datas", new JsonArray()));
				}
				
				// 数据量大activemq无法承载
				if (datas.size() > 5) {
					JsonArray converted = new JsonArray();

					Iterator<Object> itdata = datas.iterator();
					while (itdata.hasNext()) {
						JsonObject next = (JsonObject) itdata.next();
						
						next.remove("payload");
						
						converted.add(next);
					}
					
					datas = converted;
				}
				
				// 超过10条分多次返回
				if (datas != null && datas.size() > 5) {
					Iterator<Object> itdata = datas.iterator();
					JsonArray subdatas = new JsonArray();
					while (itdata.hasNext()) {
						
						if (subdatas.size() < 5) {
							subdatas.add((JsonObject) itdata.next());
						} else {
							JsonObject nextctx = new JsonObject().put("more", itdata.hasNext()).put("context", new JsonObject()
									.put("from", from)
									.put("header", header)
									.put("datas", subdatas));
							
							MessageProducer<JsonObject> producer = bridge.createProducer(nextTask);
							producer.send(new JsonObject().put("body", nextctx));
							producer.end();
							
							if (config().getBoolean("log.info", Boolean.FALSE)) {
								System.out.println(
										"Consumer " + consumer + " send to [" + nextTask + "] result [" + nextctx.encode() + "(" + nextctx.toBuffer().length() + ")]");
							}
							
							subdatas = new JsonArray();
						}
					}
					
					if (subdatas.size() > 0) {
						JsonObject nextctx = new JsonObject().put("more", Boolean.FALSE).put("context", new JsonObject()
								.put("from", from)
								.put("header", header)
								.put("datas", subdatas));
						
						MessageProducer<JsonObject> producer = bridge.createProducer(nextTask);
						producer.send(new JsonObject().put("body", nextctx));
						producer.end();
						
						if (config().getBoolean("log.info", Boolean.FALSE)) {
							System.out.println(
									"Consumer " + consumer + " send to [" + nextTask + "] result [" + nextctx.encode() + "(" + nextctx.toBuffer().length() + ")]");
						}						
					}
				} else {
					JsonObject nextctx = new JsonObject().put("context", new JsonObject()
							.put("from", from)
							.put("header", header)
							.put("datas", datas));
					
					MessageProducer<JsonObject> producer = bridge.createProducer(nextTask);
					producer.send(new JsonObject().put("body", nextctx));
					producer.end();
					
					if (config().getBoolean("log.info", Boolean.FALSE)) {
						System.out.println(
								"Consumer " + consumer + " send to [" + nextTask + "] result [" + nextctx.encode() + "(" + nextctx.toBuffer().length() + ")]");
					}
				}
			} else {
				handler.cause().printStackTrace();
				
				JsonObject nextctx = new JsonObject().put("context", new JsonObject()
						.put("from", from)
						.put("header", header)
						.put("datas", new JsonArray()));
				
				MessageProducer<JsonObject> producer = bridge.createProducer(nextTask);
				producer.send(new JsonObject().put("body", nextctx));
				producer.end();
				
				if (config().getBoolean("log.info", Boolean.FALSE)) {
					System.out.println(
							"Consumer " + consumer + " send to [" + nextTask + "] result [" + nextctx.encode() + "(" + nextctx.toBuffer().length() + ")]");
				}
			}
		});
	}

	private Boolean compareShareStatement(JsonObject statement, String from, String datastate, String invitestate, String todostate, String comparefor) {
		Boolean changed = Boolean.FALSE;
		
		JsonObject currentstatement = statement.getJsonObject(from, new JsonObject());
		
		if ("datastate".equals(comparefor)) {
			
		}

		if ("invitestate".equals(comparefor)) {
			if ("accepted".equals(invitestate) || "rejected".equals(invitestate)) {
				if (!invitestate.equals(currentstatement.getString("invitestate", ""))) {
					changed = Boolean.TRUE;
				}
			}
		}
		
		if ("todostate".equals(comparefor)) {
			
		}
		
		return changed;
	}
	
	private JsonObject mergeShareStatement(JsonObject statement, String from, String datastate, String invitestate, String todostate) {
		JsonObject merged = new JsonObject();
		
		if (statement != null && !statement.isEmpty()) {
			if (config().getBoolean("log.debug", Boolean.FALSE)) {
				System.out.println(statement.encodePrettily());
			}
			merged.mergeIn(statement, true);
		} else {
			if (config().getBoolean("log.info", Boolean.FALSE)) {
				System.out.println("Storaged statement is empty.");
			}
		}
		
		merged.put(from, new JsonObject()
				.put("datastate", datastate)
				.put("invitestate", invitestate)
				.put("todostate", todostate)
				);

		return merged;
	}
	
	private Boolean compareChangedPayload(JsonObject origin, JsonObject current, JsonObject fields) {
		Boolean hasChangedCompare = Boolean.FALSE;
		
		JsonArray compared = fields.getJsonArray("compared", new JsonArray());

		if (compared.size() > 0) {
			JsonArray changed = comparePayload(origin, current);

			if (changed != null) {
				if (config().getBoolean("log.debug", Boolean.FALSE)) {
					System.out.println("DEBUG payload compared with changes: " + changed.encode());
				}
			}

			Iterator changedfields = changed.iterator();
			
			while(changedfields.hasNext()) {
				String field = (String) changedfields.next();
				
				if (compared.contains(field)) {
					hasChangedCompare = Boolean.TRUE;
					break;
				}
			}
		} else {
			hasChangedCompare = Boolean.TRUE;
		}
		
		return hasChangedCompare;
	}
	
	private JsonArray comparePayload(JsonObject origin, JsonObject current) {
		JsonArray changes = new JsonArray();
		
		Set<String> fieldnames = origin.fieldNames();
		
		for (String fieldname : fieldnames) {
			Object originvalue = origin.getValue(fieldname);
			Object currentvalue = current.getValue(fieldname);
			
			StringBuffer sblogger = new StringBuffer();
			sblogger.append("DEBUG ");
			sblogger.append(originvalue != null? originvalue.toString() : "null");
			sblogger.append(" <=> ");
			sblogger.append(currentvalue != null? currentvalue.toString() : "null");
			
			if (originvalue != null) {
				// 两个值不相同
				if (!originvalue.equals(currentvalue)) {
					if (config().getBoolean("log.debug", Boolean.FALSE)) {
						System.out.println(sblogger.toString());
					}
					
					changes.add(fieldname);
				}
			} else {
				// 现在的值不为null
				if (currentvalue != null) {
					if (config().getBoolean("log.debug", Boolean.FALSE)) {
						System.out.println(sblogger.toString());
					}
					
					changes.add(fieldname);
				}
			}
		}
		
		return changes;
	}
	
	private JsonObject mergePayload(JsonObject origin, JsonObject current, JsonObject fields, ExchangeMethod exchange) {
		JsonArray unshared = fields.getJsonArray("unshared", new JsonArray());
		JsonObject merged = new JsonObject();
		
		switch(exchange) {
			case OwnerToMember:
			case MemberToOwner:
			case MemberToMember:
				merged.mergeIn(origin, true);
				merged.mergeIn(removeUnsharedFields(current, unshared), true);
				break;
			case MemberToSelf:
			case OwnerToOwner:
				merged = current;
				break;
		}

		return merged;
	}
	
	private JsonObject removeUnsharedFields(JsonObject in, JsonArray unshared) {
		JsonObject removed = new JsonObject();
		
		removed.mergeIn(in, true);
		
		Iterator<Object> unshares = unshared.iterator();
		
		while(unshares.hasNext()) {
			String key = (String) unshares.next();
			removed.remove(key);
		}
		
		return removed;
	}
	
	/**
	 * 比较参与人变化
	 * 
	 * 获得增加人员、删除人员和不变人员
	 * 
	 * @param before
	 * @param after
	 * @return
	 */
	private JsonObject compare(JsonArray before, JsonArray after) {
		JsonArray compacked = new JsonArray();

		compacked.addAll(before);
		compacked.addAll(after);
		
		List<String> list = compacked.getList();
		
		Collections.sort(list, new Comparator<String>() {
			@Override
			public int compare(String a, String b) {
				return a.compareTo(b);
			}
		});
		
		JsonArray added = new JsonArray();
		JsonArray updated = new JsonArray();
		JsonArray removed = new JsonArray();
		
		String pre = "";
		for (String one : list) {
			if ("".equals(pre)) {
				pre = one;
				continue;
			}
			
			if (pre.equals(one)) {
				updated.add(pre);
				pre = "";
				continue;
			} else {
				if (before.contains(pre)) {
					removed.add(pre);
				} else {
					added.add(pre);
				}

				pre = one;
			}
		}
		
		if (!"".equals(pre)) {
			if (before.contains(pre)) {
				removed.add(pre);
			} else {
				added.add(pre);
			}
		}
		
		JsonObject compared = new JsonObject();
		
		compared.put("added", added);
		compared.put("updated", updated);
		compared.put("removed", removed);
		
		return compared;
	}
	
	private void connectStompServer() {
		bridge.start(config().getString("stomp.server.host", "sa-amq"),
			config().getInteger("stomp.server.port", 5672), res -> {
				if (res.failed()) {
					res.cause().printStackTrace();
					if (!config().getBoolean("debug", true)) {
						connectStompServer();
					}
				} else {
					if (config().getBoolean("log.info", Boolean.FALSE)) {
						System.out.println("Stomp server connected.");
					}
					subscribeTrigger(config().getString("amq.app.id", "sas"));
				}
			});
	}
	
}
