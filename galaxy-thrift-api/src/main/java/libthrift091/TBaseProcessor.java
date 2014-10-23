package libthrift091;

import java.util.Collections;
import java.util.Map;

import libthrift091.protocol.TMessage;
import libthrift091.protocol.TMessageType;
import libthrift091.protocol.TProtocol;
import libthrift091.protocol.TProtocolUtil;
import libthrift091.protocol.TType;

public abstract class TBaseProcessor<I> implements TProcessor {
  private final I iface;
  private final Map<String,ProcessFunction<I, ? extends TBase>> processMap;

  protected TBaseProcessor(I iface, Map<String, ProcessFunction<I, ? extends TBase>> processFunctionMap) {
    this.iface = iface;
    this.processMap = processFunctionMap;
  }

  public Map<String,ProcessFunction<I, ? extends TBase>> getProcessMapView() {
    return Collections.unmodifiableMap(processMap);
  }

  @Override
  public boolean process(TProtocol in, TProtocol out) throws TException {
    TMessage msg = in.readMessageBegin();
    ProcessFunction fn = processMap.get(msg.name);
    if (fn == null) {
      TProtocolUtil.skip(in, TType.STRUCT);
      in.readMessageEnd();
      TApplicationException x = new TApplicationException(TApplicationException.UNKNOWN_METHOD, "Invalid method name: '"+msg.name+"'");
      out.writeMessageBegin(new TMessage(msg.name, TMessageType.EXCEPTION, msg.seqid));
      x.write(out);
      out.writeMessageEnd();
      out.getTransport().flush();
      return true;
    }
    fn.process(msg.seqid, in, out, iface);
    return true;
  }
}
