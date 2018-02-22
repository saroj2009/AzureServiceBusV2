using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using Microsoft.ServiceBus.Messaging;
using Microsoft.ServiceBus;

namespace ServiceBusTestWebApp.Controllers
{
    public class HomeController : Controller
    {
        [HttpGet]
        public ActionResult About()
        {
            var connectionString = "Endpoint=sb://skptestsrvcbus1.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=ch2Rrgub1vLIVYgoZcolaPHvJyZ/4R0XQ5w7+gRCEuM=";
            var queueName = "topic1";
            TestTopic(connectionString, queueName, "aa");
            return View();
        }
        [HttpPost]
        [ActionName("About")]
        public ActionResult About2()
        {
            var connectionString = "Endpoint=sb://skptestsrvcbus1.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=ch2Rrgub1vLIVYgoZcolaPHvJyZ/4R0XQ5w7+gRCEuM=";
            var queueName = "topic1";
            TestTopic(connectionString, queueName, "aa");
            return View();
        }
        [HttpGet]
        public ActionResult Index()
        {
            return View();
        }
        [HttpPost]
        [ActionName("Index")]
        public ActionResult Index(FormCollection form, string Send)
        {
            var connectionString = form["txtEndpoint"].ToString();
            var queueName = form["txtQueueName"].ToString();

            var namespaceManager = NamespaceManager.CreateFromConnectionString(connectionString);
            if (!namespaceManager.QueueExists(queueName))
            {
                namespaceManager.CreateQueue(queueName);
            }

            TempData["connectionString"] = connectionString;
            TempData["queueName"] = queueName;
            ViewBag.Msg = "Service Bus Endpoint has been saved suceessfully.";
            //return View();
            return RedirectToAction("SendMsg");
        }
        public ActionResult SendMsg()
        {
            string connectionString = "";
            string queueName = "";
            if (TempData["connectionString"] != null)
            {
                connectionString = Convert.ToString(TempData["connectionString"]);
                TempData.Keep("connectionString");
            }
            if (TempData["queueName"] != null)
            {
                queueName = Convert.ToString(TempData["queueName"]);
                TempData.Keep("queueName");
            }
            if (TempData["connectionString"] == null || TempData["queueName"] == null)
            {
                ViewBag.Msg = "Please enter valid Service Bus Endpoint.";
                return View("Index");
            }
            getTotalMsg(connectionString, queueName);
            ViewBag.Endpoint = connectionString;
            ViewBag.QueueName = queueName;
            return View();
        }
        [HttpPost]
        [ActionName("SendMsg")]
        public ActionResult SendMsg(FormCollection form)
        {
            var sendMsg = form["txtSend"].ToString();
            string connectionString = "";
            string queueName = "";
            if (TempData["connectionString"] != null)
            {
                connectionString = Convert.ToString(TempData["connectionString"]);
                TempData.Keep("connectionString");
            }
            if (TempData["queueName"] != null)
            {
                queueName = Convert.ToString(TempData["queueName"]);
                TempData.Keep("queueName");
            }
            ViewBag.Endpoint = connectionString;
            ViewBag.QueueName = queueName;
            SetQueueValue(connectionString, queueName, sendMsg);
            getTotalMsg(connectionString, queueName);
            return View();
        }
        [HttpGet]
        public ActionResult GetMsg()
        {
            //string connectionString = "";
            //string queueName = "";
            //if (TempData["connectionString"] != null)
            //{
            //    connectionString = Convert.ToString(TempData["connectionString"]);
            //    TempData.Keep("connectionString");
            //}
            //if (TempData["queueName"] != null)
            //{
            //    queueName = Convert.ToString(TempData["queueName"]);
            //    TempData.Keep("queueName");
            //}
            //if (TempData["connectionString"] == null || TempData["queueName"] == null)
            //{
            //    ViewBag.Msg = "Please enter valid Service Bus Endpoint.";
            //    return View("Index");
            //}
            //getTotalMsg(connectionString, queueName);
            //ViewBag.Endpoint = connectionString;
            //ViewBag.QueueName = queueName;
            //getQueueValue(connectionString, queueName);
            return View();
        }
        [HttpPost]
        [ActionName("GetMsg")]
        public ActionResult GetMsg(FormCollection form)
        {
            string connectionString = "";
            string queueName = "";
            if (TempData["connectionString"] != null)
            {
                connectionString = Convert.ToString(TempData["connectionString"]);
                TempData.Keep("connectionString");
            }
            if (TempData["queueName"] != null)
            {
                queueName = Convert.ToString(TempData["queueName"]);
                TempData.Keep("queueName");
            }
            getTotalMsg(connectionString, queueName);
            ViewBag.Endpoint = connectionString;
            ViewBag.QueueName = queueName;
            getQueueValue(connectionString, queueName);
            return View();
        }
        [NonAction]
        public void SetQueueValue(string strEndpoint, string strQueueName, string strValue)
        {
            var sendMsg = strValue;
            ViewBag.Endpoint = strEndpoint;
            ViewBag.QueueName = strQueueName;

            var client = QueueClient.CreateFromConnectionString(strEndpoint, strQueueName);
            var message = new BrokeredMessage(sendMsg);
            client.Send(message);
            ViewBag.Msg = "Message has been sent suceessfully.";
        }
        [NonAction]
        public void getQueueValue(string strEndpoint, string strQueueName)
        {
            var connectionString = strEndpoint;
            var queueName = strQueueName;
            ViewBag.Endpoint = connectionString;
            ViewBag.QueueName = queueName;

            var client = QueueClient.CreateFromConnectionString(connectionString, queueName);
            var msg3 = "";

            BrokeredMessage message = null;
            NamespaceManager namespaceManager = NamespaceManager.Create();
            while (true)
            {
                try
                {
                    //receive messages from Queue 
                    message = client.Receive(TimeSpan.FromSeconds(5));
                    if (message != null)
                    {
                        //Console.WriteLine(string.Format("Message received: Id = {0}, Body = {1}", message.MessageId, message.GetBody<string>()));
                        var t = message.MessageId;
                        msg3 = message.GetBody<string>();
                        message.Complete();
                        break;
                    }
                    else
                    {
                        msg3 = "";
                        ViewBag.Msg = "No active message is avilable in service bus queue.";
                        break;
                    }
                }
                catch (MessagingException e)
                {
                    if (!e.IsTransient)
                    {
                        Console.WriteLine(e.Message);
                        throw;
                    }
                    else
                    {
                        //HandleTransientErrors(e);
                    }
                }
            }
            client.Close();
            ViewBag.Qmsg2 = msg3;

        }
        [NonAction]
        public void getTotalMsg(string connectionString, string queueName)
        {
            var nsmgr = Microsoft.ServiceBus.NamespaceManager.CreateFromConnectionString(connectionString);
            long count = nsmgr.GetQueue(queueName).MessageCount;
            ViewBag.MsgCount = count;
        }
        [NonAction]
        public void TestTopic(string strEndpoint, string strQueueName, string strValue)
        {
            //        string connectionString =
            //CloudConfigurationManager.GetSetting("Microsoft.ServiceBus.ConnectionString");
            var connectionString = strEndpoint;

            var namespaceManager =
                NamespaceManager.CreateFromConnectionString(connectionString);

            if (!namespaceManager.TopicExists("azservicetopic"))
            {
                namespaceManager.CreateTopic("azservicetopic");
            }

            if (!namespaceManager.SubscriptionExists("azservicetopic", "AllMessages"))
            {
                namespaceManager.CreateSubscription("azservicetopic", "AllMessages");
            }

            // Create a "HighProperty" filtered subscription.
            SqlFilter highPropertyFilter =
               new SqlFilter("MessageNumber > 3");

            namespaceManager.CreateSubscription("azservicetopic",
               "HighProperty",
               highPropertyFilter);

            // Create a "LowProperty" filtered subscription.
            SqlFilter lowPropertyFilter =
               new SqlFilter("MessageNumber <= 3");

            namespaceManager.CreateSubscription("azservicetopic",
               "LowProperty",
               lowPropertyFilter);

            SendMessage(connectionString);
            //ReadMessage(connectionString);
        }
        [NonAction]
        private static void SendMessage(string connStr)
        {
            TopicClient Client =
    TopicClient.CreateFromConnectionString(connStr, "azservicetopic");
            for (int i = 0; i < 5; i++)
            {
                // Create message, passing a string message for the body.
                BrokeredMessage message = new BrokeredMessage("This is sample message " + i);

                // Set additional custom app-specific property.
                message.Properties["MessageNumber"] = i;

                // Send message to the topic.
                Client.Send(message);
            }

        }
    }
}