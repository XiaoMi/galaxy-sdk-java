package com.xiaomi.infra.galaxy.talos.client;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import libthrift091.protocol.TCompactProtocol;
import libthrift091.protocol.TProtocol;
import org.apache.http.client.HttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.galaxy.rpc.thrift.ThriftProtocol;
import com.xiaomi.infra.galaxy.rpc.util.clock.AdjustableClock;

public class ThreadSafeClient<IFace, Impl> {
  private static class ThreadSafeInvocationHandler<IFace, Impl> implements InvocationHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ThreadSafeInvocationHandler.class);
    private static final Map<Class, Constructor> ctorCache =
        new ConcurrentHashMap<Class, Constructor>();
    private final HttpClient client;
    private final Map<String, String> customHeaders;
    private final Credential credential;
    private final AdjustableClock clock;
    final Class<IFace> ifaceClass;
    final Class<Impl> implClass;
    final String url;
    private int socketTimeout = 0;
    private int connTimeout = 0;
    private boolean supportAccountKey = false;
    private String sid;

    private ThreadSafeInvocationHandler(HttpClient client, Map<String, String> customHeaders,
        Credential credential, AdjustableClock clock, Class<IFace> ifaceClass,
        Class<Impl> implClass, String url, int socketTimeout, int connTimeout,
        boolean supportAccountKey, String sid) {
      this.client = client;
      this.customHeaders = customHeaders;
      this.credential = credential;
      this.clock = clock;
      this.ifaceClass = ifaceClass;
      this.implClass = implClass;
      this.url = url;
      this.socketTimeout = socketTimeout;
      this.connTimeout = connTimeout;
      this.supportAccountKey = supportAccountKey;
      this.sid = sid;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      try {
        TalosHttpClient talosHttpClient = new TalosHttpClient(url, client, this.credential, clock);
        talosHttpClient.setSocketTimeout(socketTimeout)
            .setConnectTimeout(connTimeout)
            .setProtocol(ThriftProtocol.TCOMPACT)
            .setQueryString("type=" + method.getName())
            .setSupportAccountKey(supportAccountKey)
            .setSid(sid);

        TProtocol proto = new TCompactProtocol(talosHttpClient);

        if (customHeaders != null) {
          for (Map.Entry<String, String> header : customHeaders.entrySet()) {
            talosHttpClient.setCustomHeader(header.getKey(), header.getValue());
          }
        }

        Impl client = getDeclaredConstructor(implClass).newInstance(proto);
        return method.invoke(client, args);
      } catch (InvocationTargetException e) {
        throw e.getCause();
      }
    }

    public static <T> Constructor<T> getDeclaredConstructor(Class<T> clazz)
        throws NoSuchMethodException {
      Constructor ctor = ctorCache.get(clazz);
      if (ctor == null) {
        ctor = clazz.getDeclaredConstructor(TProtocol.class);
        ctorCache.put(clazz, ctor);
      }
      return ctor;
    }
  }

  /**
   * Create client wrapper which automatically retry the RPC calls for retryable errors until
   * success or reaches max retry time.
   */
  @SuppressWarnings("unchecked")
  public static <IFace, Impl> IFace getClient(HttpClient client, Map<String, String> customHeaders,
      Credential credential, AdjustableClock clock, Class<IFace> ifaceClass, Class<Impl> implClass,
      String url, int socketTimeout, int connTimeout, boolean supportAccountKey, String sid) {
    return (IFace) Proxy.newProxyInstance(ThreadSafeClient.class.getClassLoader(),
        new Class[] { ifaceClass },
        new ThreadSafeInvocationHandler<IFace, Impl>(client, customHeaders, credential, clock,
            ifaceClass, implClass, url, socketTimeout, connTimeout, supportAccountKey, sid)
    );
  }
}
