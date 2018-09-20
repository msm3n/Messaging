using System;
using System.Threading;

namespace Lykke.Messaging.Utils
{
    public class Helper
    {
        public static Action CallOnlyOnce(Action action)
        {
            var alreadyCalled = 0;
            Action ret = () =>
            {
                if (Interlocked.Exchange(ref alreadyCalled, 1) != 0)
                    return;
                action();
            };

            return ret;
        } 
    }
}