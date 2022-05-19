using System;
using System.Threading;
using System.Threading.Tasks;

namespace GResearch.Armada.Client
{
    public static class TaskExtensions
    {
        /// <summary>
        /// Extension method to let a Task timeout with a TimeoutException iof it does not complete before the given timeout
        /// </summary>
        /// <param name="task">Task to apply the timeout to</param>
        /// <param name="timeout">Timespan after which the task will fail with a TimeoutException</param>
        /// <typeparam name="TResult">Result type returned by the task</typeparam>
        /// <returns>Modified Task that will </returns>
        public static async Task<TResult> TimeoutAfter<TResult>(this Task<TResult> task, TimeSpan timeout) {

            using (var timeoutCancellationTokenSource = new CancellationTokenSource()) {
                var completedTask = await Task.WhenAny(task, Task.Delay(timeout, timeoutCancellationTokenSource.Token));
                if (completedTask == task) {
                    timeoutCancellationTokenSource.Cancel();
                    return await task;  // thi si needed to propagate exceptions
                } else {
                    throw new TimeoutException($"Operation timed out after {timeout}");
                }
            }
        }
    }
}
