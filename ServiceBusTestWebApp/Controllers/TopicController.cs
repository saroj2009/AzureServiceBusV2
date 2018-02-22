using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace ServiceBusTestWebApp.Controllers
{
    public class TopicController : Controller
    {
        // GET: Topic
        public ActionResult Index()
        {
            //var connectionString = "Endpoint=sb://skptestsrvcbus1.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=ch2Rrgub1vLIVYgoZcolaPHvJyZ/4R0XQ5w7+gRCEuM=";
            //var queueName = "topic1";
            //TestTopic(connectionString, queueName, "aa");
            return View();
        }
        [HttpPost]
        [ActionName("Index")]
        public ActionResult Index(FormCollection form, string Send)
        {
            var connectionString = form["txtEndpoint"].ToString();
            var toipcName = form["txtQueueName"].ToString();

            var namespaceManager = NamespaceManager.CreateFromConnectionString(connectionString);
           
            //if (!namespaceManager.TopicExists(toipcName))
            //{
            //    namespaceManager.CreateTopic(toipcName);
            //}
            TestTopic(connectionString, toipcName, "");
            TempData["connectionString2"] = connectionString;
            TempData["queueName2"] = toipcName;
            ViewBag.Msg = "Service Bus Endpoint has been saved suceessfully.";
            //return View();
            return RedirectToAction("SendMsg");
        }
        public ActionResult SendMsg()
        {
            string connectionString = "";
            string queueName = "";
            if (TempData["connectionString2"] != null)
            {
                connectionString = Convert.ToString(TempData["connectionString2"]);
                TempData.Keep("connectionString2");
            }
            if (TempData["queueName2"] != null)
            {
                queueName = Convert.ToString(TempData["queueName2"]);
                TempData.Keep("queueName2");
            }
            if (TempData["connectionString2"] == null || TempData["queueName2"] == null)
            {
                ViewBag.Msg = "Please enter valid Service Bus Endpoint.";
                return View("Index");
            }
            //getTotalMsg(connectionString, queueName);
            ViewBag.Endpoint2 = connectionString;
            ViewBag.QueueName2 = queueName;
            ViewBag.TopicMsg = "";
            return View();
        }
        [HttpPost]
        [ActionName("SendMsg")]
        public ActionResult SendMsg(FormCollection form)
        {
            //var sendMsg = form["txtSend"].ToString();
            string connectionString = "";
            string topicName = "";
            if (TempData["connectionString2"] != null)
            {
                connectionString = Convert.ToString(TempData["connectionString2"]);
                TempData.Keep("connectionString2");
            }
            if (TempData["queueName2"] != null)
            {
                topicName = Convert.ToString(TempData["queueName2"]);
                TempData.Keep("queueName2");
            }
            ViewBag.Endpoint2 = connectionString;
            ViewBag.QueueName2 = topicName;
            //SetQueueValue(connectionString, queueName, sendMsg);
            //getTotalMsg(connectionString, queueName);
            TestTopic(connectionString, topicName, "");
            SendMessage(connectionString, topicName);
            ViewBag.TopicMsg = "Message has been sent suceessfully.";
            return View();
        }
        [HttpGet]
        public ActionResult GetMsg()
        {
            string connectionString = "";
            string queueName = "";
            if (TempData["connectionString2"] != null)
            {
                connectionString = Convert.ToString(TempData["connectionString2"]);
                TempData.Keep("connectionString2");
            }
            if (TempData["queueName2"] != null)
            {
                queueName = Convert.ToString(TempData["queueName2"]);
                TempData.Keep("queueName2");
            }
            if (TempData["connectionString2"] == null || TempData["queueName2"] == null)
            {
                ViewBag.Msg = "Please enter valid Service Bus Endpoint.";
                return View("Index");
            }
            //getTotalMsg(connectionString, queueName);
            ViewBag.Endpoint2 = connectionString;
            ViewBag.QueueName2 = queueName;
            ViewBag.Qmsg4 = "";
           // getQueueValue(connectionString, queueName);
            return View();
        }
       
        [NonAction]
        public void getQueueValue(string strEndpoint, string strTopicName, string subscriptionName)
        {
            var connectionString = strEndpoint;
            var queueName = strTopicName;
            ViewBag.Endpoint = connectionString;
            ViewBag.QueueName = queueName;

            SubscriptionClient client = SubscriptionClient.CreateFromConnectionString(strEndpoint, strTopicName, subscriptionName);
            var msg3 = "";

            BrokeredMessage message = null;
            NamespaceManager namespaceManager = NamespaceManager.Create();
            while (true)
            {
                try
                {
                    //receive messages from Queue 
                    message = client.Receive(TimeSpan.FromSeconds(5));
                    ViewBag.Msg12 = "No active message is avilable in service bus queue.";
                    if (message != null)
                    {
                        //Console.WriteLine(string.Format("Message received: Id = {0}, Body = {1}", message.MessageId, message.GetBody<string>()));
                        var t = message.MessageId;
                        if (msg3 == "")
                            msg3 = message.GetBody<string>();
                        else
                        msg3 = msg3+", "+ message.GetBody<string>();
                        message.Complete();
                        ViewBag.Msg12 = "";
                       // break;
                    }
                    else
                    {
                        //msg3 = "";
                        
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
            ViewBag.Qmsg4 = msg3;

        }
        [HttpPost]
        [ActionName("GetMsg")]
        public ActionResult GetMsg(FormCollection form)
        {
            string connectionString = "";
            string topicName = "";
            var subscriptionName = form["txtSubscriptionName"].ToString();
            if (TempData["connectionString2"] != null)
            {
                connectionString = Convert.ToString(TempData["connectionString2"]);
                TempData.Keep("connectionString2");
            }
            if (TempData["queueName2"] != null)
            {
                topicName = Convert.ToString(TempData["queueName2"]);
                TempData.Keep("queueName2");
            }
            //getTotalMsg(connectionString, queueName);
            ViewBag.Endpoint2 = connectionString;
            ViewBag.QueueName2 = topicName;
            ViewBag.Subscription = subscriptionName;
           getQueueValue(connectionString, topicName, subscriptionName);
            return View();
        }
        [NonAction]
        public void TestTopic(string strEndpoint, string strTopicName, string strValue)
        {
            //        string connectionString =
            //CloudConfigurationManager.GetSetting("Microsoft.ServiceBus.ConnectionString");
            var connectionString = strEndpoint;

            var namespaceManager =
                NamespaceManager.CreateFromConnectionString(connectionString);

            if (!namespaceManager.TopicExists(strTopicName))
            {
                namespaceManager.CreateTopic(strTopicName);
            }

            if (!namespaceManager.SubscriptionExists(strTopicName, "AllMessages"))
            {
                namespaceManager.CreateSubscription(strTopicName, "AllMessages");
            }

            // Create a "HighProperty" filtered subscription.
            SqlFilter highPropertyFilter =
               new SqlFilter("MessageNumber > 3");

            if (!namespaceManager.SubscriptionExists(strTopicName, "HighProperty"))
            {
                namespaceManager.CreateSubscription(strTopicName,
               "HighProperty",
               highPropertyFilter);
            }

            // Create a "LowProperty" filtered subscription.
            SqlFilter lowPropertyFilter =
               new SqlFilter("MessageNumber <= 3");
            if (!namespaceManager.SubscriptionExists(strTopicName, "LowProperty"))
            {
                namespaceManager.CreateSubscription(strTopicName,
               "LowProperty",
               lowPropertyFilter);
            }

           // SendMessage(connectionString, strTopicName);
            //ReadMessage(connectionString);
        }
        [NonAction]
        private static void SendMessage(string connStr, string TopicName)
        {
            TopicClient Client =
    TopicClient.CreateFromConnectionString(connStr, TopicName);
            for (int i = 0; i < 5; i++)
            {
                // Create message, passing a string message for the body.
                BrokeredMessage message = new BrokeredMessage("Message " + i);

                // Set additional custom app-specific property.
                message.Properties["MessageNumber"] = i;

                // Send message to the topic.
                Client.Send(message);
            }

        }
    }
}