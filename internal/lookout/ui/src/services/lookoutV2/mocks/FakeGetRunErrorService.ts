import { simulateApiWait } from "../../../utils/fakeJobsUtils"
import { IGetRunErrorService } from "../GetRunErrorService"

export class FakeGetRunErrorService implements IGetRunErrorService {
  constructor(private simulateApiWait = true) {}

  async getRunError(runId: string, signal?: AbortSignal): Promise<string> {
    if (this.simulateApiWait) {
      await simulateApiWait(signal)
    }
    if (runId === "doesnotexist") {
      throw new Error("Failed to retrieve job run because of reasons")
    }
    return Promise.resolve(
      "javax.servlet.ServletException: Something bad happened\n" +
        "    at com.example.myproject.OpenSessionInViewFilter.doFilter(OpenSessionInViewFilter.java:60)\n" +
        "    at org.mortbay.jetty.servlet.ServletHandler$CachedChain.doFilter(ServletHandler.java:1157)\n" +
        "    at com.example.myproject.ExceptionHandlerFilter.doFilter(ExceptionHandlerFilter.java:28)\n" +
        "    at org.mortbay.jetty.servlet.ServletHandler$CachedChain.doFilter(ServletHandler.java:1157)\n" +
        "    at com.example.myproject.OutputBufferFilter.doFilter(OutputBufferFilter.java:33)\n" +
        "    at org.mortbay.jetty.servlet.ServletHandler$CachedChain.doFilter(ServletHandler.java:1157)\n" +
        "    at org.mortbay.jetty.servlet.ServletHandler.handle(ServletHandler.java:388)\n" +
        "    at org.mortbay.jetty.security.SecurityHandler.handle(SecurityHandler.java:216)\n" +
        "    at org.mortbay.jetty.servlet.SessionHandler.handle(SessionHandler.java:182)\n" +
        "    at org.mortbay.jetty.handler.ContextHandler.handle(ContextHandler.java:765)\n" +
        "    at org.mortbay.jetty.webapp.WebAppContext.handle(WebAppContext.java:418)\n" +
        "    at org.mortbay.jetty.handler.HandlerWrapper.handle(HandlerWrapper.java:152)\n" +
        "    at org.mortbay.jetty.Server.handle(Server.java:326)\n" +
        "    at org.mortbay.jetty.HttpConnection.handleRequest(HttpConnection.java:542)\n" +
        "    at org.mortbay.jetty.HttpConnection$RequestHandler.content(HttpConnection.java:943)\n" +
        "    at org.mortbay.jetty.HttpParser.parseNext(HttpParser.java:756)\n" +
        "    at org.mortbay.jetty.HttpParser.parseAvailable(HttpParser.java:218)\n" +
        "    at org.mortbay.jetty.HttpConnection.handle(HttpConnection.java:404)\n" +
        "    at org.mortbay.jetty.bio.SocketConnector$Connection.run(SocketConnector.java:228)\n" +
        "    at org.mortbay.thread.QueuedThreadPool$PoolThread.run(QueuedThreadPool.java:582)\n" +
        "Caused by: com.example.myproject.MyProjectServletException\n" +
        "    at com.example.myproject.MyServlet.doPost(MyServlet.java:169)\n" +
        "    at javax.servlet.http.HttpServlet.service(HttpServlet.java:727)\n" +
        "    at javax.servlet.http.HttpServlet.service(HttpServlet.java:820)\n" +
        "    at org.mortbay.jetty.servlet.ServletHolder.handle(ServletHolder.java:511)\n" +
        "    at org.mortbay.jetty.servlet.ServletHandler$CachedChain.doFilter(ServletHandler.java:1166)\n" +
        "    at com.example.myproject.OpenSessionInViewFilter.doFilter(OpenSessionInViewFilter.java:30)\n" +
        "    ... 27 more\n" +
        "Caused by: org.hibernate.exception.ConstraintViolationException: could not insert: [com.example.myproject.MyEntity]\n" +
        "    at org.hibernate.exception.SQLStateConverter.convert(SQLStateConverter.java:96)\n" +
        "    at org.hibernate.exception.JDBCExceptionHelper.convert(JDBCExceptionHelper.java:66)\n" +
        "    at org.hibernate.id.insert.AbstractSelectingDelegate.performInsert(AbstractSelectingDelegate.java:64)\n" +
        "    at org.hibernate.persister.entity.AbstractEntityPersister.insert(AbstractEntityPersister.java:2329)\n" +
        "    at org.hibernate.persister.entity.AbstractEntityPersister.insert(AbstractEntityPersister.java:2822)\n" +
        "    at org.hibernate.action.EntityIdentityInsertAction.execute(EntityIdentityInsertAction.java:71)\n" +
        "    at org.hibernate.engine.ActionQueue.execute(ActionQueue.java:268)\n" +
        "    at org.hibernate.event.def.AbstractSaveEventListener.performSaveOrReplicate(AbstractSaveEventListener.java:321)\n" +
        "    at org.hibernate.event.def.AbstractSaveEventListener.performSave(AbstractSaveEventListener.java:204)\n" +
        "    at org.hibernate.event.def.AbstractSaveEventListener.saveWithGeneratedId(AbstractSaveEventListener.java:130)\n" +
        "    at org.hibernate.event.def.DefaultSaveOrUpdateEventListener.saveWithGeneratedOrRequestedId(DefaultSaveOrUpdateEventListener.java:210)\n" +
        "    at org.hibernate.event.def.DefaultSaveEventListener.saveWithGeneratedOrRequestedId(DefaultSaveEventListener.java:56)\n" +
        "    at org.hibernate.event.def.DefaultSaveOrUpdateEventListener.entityIsTransient(DefaultSaveOrUpdateEventListener.java:195)\n" +
        "    at org.hibernate.event.def.DefaultSaveEventListener.performSaveOrUpdate(DefaultSaveEventListener.java:50)\n" +
        "    at org.hibernate.event.def.DefaultSaveOrUpdateEventListener.onSaveOrUpdate(DefaultSaveOrUpdateEventListener.java:93)\n" +
        "    at org.hibernate.impl.SessionImpl.fireSave(SessionImpl.java:705)\n" +
        "    at org.hibernate.impl.SessionImpl.save(SessionImpl.java:693)\n" +
        "    at org.hibernate.impl.SessionImpl.save(SessionImpl.java:689)\n" +
        "    at sun.reflect.GeneratedMethodAccessor5.invoke(Unknown Source)\n" +
        "    at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:25)\n" +
        "    at java.lang.reflect.Method.invoke(Method.java:597)\n" +
        "    at org.hibernate.context.ThreadLocalSessionContext$TransactionProtectionWrapper.invoke(ThreadLocalSessionContext.java:344)\n" +
        "    at $Proxy19.save(Unknown Source)\n" +
        "    at com.example.myproject.MyEntityService.save(MyEntityService.java:59) <-- relevant call (see notes below)\n" +
        "    at com.example.myproject.MyServlet.doPost(MyServlet.java:164)\n" +
        "    ... 32 more\n" +
        "Caused by: java.sql.SQLException: Violation of unique constraint MY_ENTITY_UK_1: duplicate value(s) for column(s) MY_COLUMN in statement [...]\n" +
        "    at org.hsqldb.jdbc.Util.throwError(Unknown Source)\n" +
        "    at org.hsqldb.jdbc.jdbcPreparedStatement.executeUpdate(Unknown Source)\n" +
        "    at com.mchange.v2.c3p0.impl.NewProxyPreparedStatement.executeUpdate(NewProxyPreparedStatement.java:105)\n" +
        "    at org.hibernate.id.insert.AbstractSelectingDelegate.performInsert(AbstractSelectingDelegate.java:57)\n" +
        "    ... 54 more",
    )
  }
}
