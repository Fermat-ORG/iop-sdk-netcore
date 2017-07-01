using System;
using System.Collections.Generic;
using System.Text;

namespace IopAppCore.ExecutionEvents
{
  /// <summary>
  /// Description of a single event in the execution flow.
  /// </summary>
  public class ExecutionEvent
  {
    /// <summary>Name of the event.</summary>
    public string Name;

    /// <summary>Timestamp of the event.</summary>
    public DateTime Timestamp;

    /// <summary>
    /// Exclusive event is an event that can only be added once to the context.
    /// Subsequent attempts to add this event to the context will fail.
    /// </summary>
    public bool IsExclusive;


    /// <summary>
    /// Initializes a new instance of the object.
    /// </summary>
    /// <param name="Name">Name of the event.</param>
    /// <param name="Exclusive">true if the barrier should be exclusive, false otherwise.</param>
    public ExecutionEvent(string Name, bool Exclusive = false)
    {
      this.Name = Name;
      Timestamp = DateTime.UtcNow;
      IsExclusive = Exclusive;
    }
  }
}
