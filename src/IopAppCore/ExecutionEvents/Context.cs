using IopCommon;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace IopAppCore.ExecutionEvents
{
  /// <summary>
  /// Execution context describes the origin of series of execution events.
  /// </summary>
  public class Context
  {
    /// <summary>Class logger.</summary>
    private static Logger clog = new Logger("IopAppCore.ExecutionEvents.Context");

    /// <summary>Instance logger.</summary>
    private Logger log;

    /// <summary>Unique context identifier.</summary>
    public string Id;

    /// <summary>Creation time of the context.</summary>
    public DateTime CreationTime;

    /// <summary>Lock object to protect access to contextNameCount.</summary>
    private static object contextNameCountLock = new object();

    /// <summary>
    /// List of existing context names and number of their instances.
    /// <para>With each new instance, the number is incremented and is used as a suffix of the identifier of the new instance.</para>
    /// </summary>
    private static Dictionary<string, int> contextNameCount = new Dictionary<string, int>(StringComparer.Ordinal);


    /// <summary>Link to LRU list of contexts.</summary>
    private LinkedListNode<string> lruLink;
    /// <summary>Link to LRU list of contexts.</summary>
    public LinkedListNode<string> LruLink { get { return lruLink; } }



    /// <summary>Async-safe execution context storage.</summary>
    public static AsyncLocal<Context> CurrentContext = new AsyncLocal<Context>();


    public Context(string Name, int Index)
    {
      Id = CreateId(Name, Index);
      log = new Logger("IopAppCore.ExecutionEvents.Context", $"[{Id}] ");

      log.Trace("(Name:'{0}',Index:{1})", Name, Index);
      CreationTime = DateTime.UtcNow;
      log.Trace("(-)");
    }


    /// <summary>
    /// Clears static parts of the context.
    /// </summary>
    public static void Clear()
    {
      clog.Trace("()");

      contextNameCount.Clear();
      CurrentContext.Value = null;

      clog.Trace("(-)");
    }

    /// <summary>
    /// Creates new instance of an execution context.
    /// </summary>
    /// <param name="Name">Name of the context from which its ID will be constructed.</param>
    /// <returns>New context instance.</returns>
    public static Context Create(string Name)
    {
      clog.Trace("(Name:'{0}')", Name);

      int index;
      lock (contextNameCountLock)
      {
        if (!contextNameCount.TryGetValue(Name, out index)) index = 0;
        index++;
        contextNameCount[Name] = index;
      }

      Context res = new Context(Name, index);
      History.AddContext(res);
      CurrentContext.Value = res;

      clog.Trace("(-):*.Id='{0}'", res.Id);
      return res;
    }


    /// <summary>
    /// Constructs context ID given its name and index.
    /// </summary>
    /// <param name="Name">Name of the context.</param>
    /// <param name="Index">Unique index number of the context name.</param>
    /// <returns>Unique context identifier.</returns>
    public static string CreateId(string Name, int Index)
    {
      return string.Format("{0}/{1}", Name, Index);
    }


    /// <summary>
    /// Sets link to LRU list of contexts.
    /// </summary>
    /// <param name="Link">Link to LRU list of contexts.</param>
    public void SetLruLink(LinkedListNode<string> Link)
    {
      lruLink = Link;
    }



    /// <summary>
    /// Adds a new exclusive event to the context.
    /// </summary>
    /// <param name="Name">Name of the event to add.</param>
    /// <returns>true if the function succeeds, false otherwise.
    /// Adding exclusive event succeeds if the event was not added to this context before.</returns>
    public bool AddExclusiveEvent(string Name)
    {
      return AddEvent(new ExecutionEvent(Name, true));
    }

    /// <summary>
    /// Adds a new non-exclusive event to the context.
    /// </summary>
    /// <param name="Name">Name of the event to add.</param>
    /// <remarks>Adding non-exclusive event will always succeeds.</remarks>
    public void AddEvent(string Name)
    {
      AddEvent(new ExecutionEvent(Name, false));
    }


    /// <summary>
    /// Adds a new event to the context.
    /// </summary>
    /// <param name="Event">Event to add.</param>
    /// <returns>true if the function succeeds, false otherwise.
    /// Adding exclusive event succeeds if the event was not added to this context before.
    /// Adding non-exclusive event will always succeeds.</returns>
    public bool AddEvent(ExecutionEvent Event)
    {
      return History.AddEvent(this, Event);
    }

    /// <summary>
    /// Finds context using its ID.
    /// </summary>
    /// <param name="Id">Context identifier.</param>
    /// <returns>Existing context or null if no context with the given identifier exists.</returns>
    public static Context GetContext(string Id)
    {
      return History.GetContext(Id);
    }

    /// <summary>
    /// Finds context using its ID and makes it the current context.
    /// </summary>
    /// <param name="Id">Context identifier.</param>
    /// <returns>Existing context or null if no context with the given identifier exists.</returns>
    public static Context GetAndSetCurrentContext(string Id)
    {
      clog.Trace("(Id:'{0}')", Id);

      Context res = History.GetContext(Id);
      CurrentContext.Value = res;

      clog.Trace("(-)");
      return res;
    }


    /// <summary>
    /// Installs a waitable task to the context so that it is possible to wait for a specific event to be created on the context.
    /// </summary>
    /// <param name="Name">Name of the event</param>
    /// <param name="TimeoutMs">Timeout in milliseconds after which the waiting fails.</param>
    /// <param name="CancellationToken">A cancellation token for the task.</param>
    /// <returns>true if the wait succeeds, false if the timeout occurs or if the cancellation token is cancelled.</returns>
    public async Task<bool> WaitEvent(string Name, int TimeoutMs, CancellationToken CancellationToken)
    {
      log.Trace("(Name:'{0}',TimeoutMs:{1})", Name, TimeoutMs);
      bool res = false;

      // Create timeout cancellation token source if the caller specified timeout.
      CancellationTokenSource timeoutTokenSource = TimeoutMs != Timeout.Infinite ? new CancellationTokenSource(TimeoutMs) : null;

      // Create a combined cancellation token source if there is at least one cancellation token - i.e. timeout cancellation, caller provided cancellation, or both.
      List<CancellationToken> cancellationTokens = new List<CancellationToken>();
      if (timeoutTokenSource != null) cancellationTokens.Add(timeoutTokenSource.Token);
      if (CancellationToken != CancellationToken.None) cancellationTokens.Add(CancellationToken);

      CancellationTokenSource cancellationTokenSource = null;
      if (cancellationTokens.Count > 0) cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationTokens.ToArray());

      CancellationTokenRegistration? cancellationTokenRegistration = null;
      try
      {
        TaskCompletionSource<bool> tcs = new TaskCompletionSource<bool>();
        if (cancellationTokenSource != null) cancellationTokenRegistration = cancellationTokenSource.Token.Register(() => { tcs.TrySetResult(false); });

        History.AddEventWaiter(this, Name, tcs);

        res = await tcs.Task;
      }
      catch (TaskCanceledException)
      {
        log.Debug("Waiting task for event '{0}' cancelled.");
      }
      catch (Exception e)
      {
        log.Error("Exception occurred: {0}", e.ToString());
      }

      if (cancellationTokenRegistration != null) cancellationTokenRegistration.Value.Dispose();
      if (cancellationTokenSource != null) cancellationTokenSource.Dispose();
      if (timeoutTokenSource != null) timeoutTokenSource.Dispose();

      log.Trace("(-):{0}", res);
      return res;
    }
  }
}
