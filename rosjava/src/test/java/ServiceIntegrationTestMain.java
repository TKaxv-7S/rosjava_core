import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.ros.RosCore;
import org.ros.exception.DuplicateServiceException;
import org.ros.exception.RemoteException;
import org.ros.exception.RosRuntimeException;
import org.ros.exception.ServiceNotFoundException;
import org.ros.namespace.GraphName;
import org.ros.node.*;
import org.ros.node.service.*;
import rosjava_test_msgs.AddTwoIntsRequest;
import rosjava_test_msgs.AddTwoIntsResponse;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static junit.framework.Assert.fail;


/**
 * @author damonkohler@google.com (Damon Kohler)
 */
public class ServiceIntegrationTestMain {

    private static final Log log = LogFactory.getLog(ServiceIntegrationTestMain.class);

    public static void main(String[] args) throws Exception {

        final String SERVICE_NAME = "/add_two_ints";

        RosCore rosCore = RosCore.newPrivate();
        rosCore.start();
        rosCore.awaitStart(1, TimeUnit.SECONDS);

        NodeConfiguration nodeConfiguration = NodeConfiguration.newPrivate(rosCore.getUri());

        NodeMainExecutor nodeMainExecutor = DefaultNodeMainExecutor.newDefault();
        NodeMainExecutor nodeClientExecutor = DefaultNodeMainExecutor.newDefault();

        final CountDownServiceServerListener<AddTwoIntsRequest, AddTwoIntsResponse> countDownServiceServerListener =
                CountDownServiceServerListener.newDefault();
        nodeMainExecutor.execute(new AbstractNodeMain() {
            @Override
            public GraphName getDefaultNodeName() {
                return GraphName.of("server");
            }

            @Override
            public void onStart(final ConnectedNode connectedNode) {
                ServiceServer<AddTwoIntsRequest, AddTwoIntsResponse> serviceServer =
                        connectedNode
                                .newServiceServer(
                                        SERVICE_NAME,
                                        rosjava_test_msgs.AddTwoInts._TYPE,
                                        new ServiceResponseBuilder<AddTwoIntsRequest, AddTwoIntsResponse>() {
                                            @Override
                                            public void build(AddTwoIntsRequest request,
                                                              AddTwoIntsResponse response) {
                                                log.info("获取A:" + request.getA() + ",获取B:" + request.getB());
                                                response.setSum(request.getA() + request.getB());
                                            }
                                        });
                try {
                    connectedNode.newServiceServer(SERVICE_NAME, rosjava_test_msgs.AddTwoInts._TYPE, null);
                    fail();
                } catch (DuplicateServiceException e) {
                    // Only one ServiceServer with a given name can be created.
                }
                serviceServer.addListener(countDownServiceServerListener);
            }
        }, nodeConfiguration);

        countDownServiceServerListener.awaitMasterRegistrationSuccess(1, TimeUnit.SECONDS);

        final CountDownLatch latch = new CountDownLatch(2);
        nodeClientExecutor.execute(new AbstractNodeMain() {
            @Override
            public GraphName getDefaultNodeName() {
                return GraphName.of("client");
            }

            @Override
            public void onStart(ConnectedNode connectedNode) {
                ServiceClient<AddTwoIntsRequest, AddTwoIntsResponse> serviceClient;
                try {
                    serviceClient = connectedNode.newServiceClient(SERVICE_NAME, rosjava_test_msgs.AddTwoInts._TYPE);
                    // Test that requesting another client for the same service returns
                    // the same instance.
                    ServiceClient<?, ?> duplicate =
                            connectedNode.newServiceClient(SERVICE_NAME, rosjava_test_msgs.AddTwoInts._TYPE);
                } catch (ServiceNotFoundException e) {
                    throw new RosRuntimeException(e);
                }
                AddTwoIntsRequest request = serviceClient.newMessage();
                request.setA(2);
                request.setB(2);
                serviceClient.call(request, new ServiceResponseListener<AddTwoIntsResponse>() {
                    @Override
                    public void onSuccess(AddTwoIntsResponse response) {
                        log.info("结果:" + response.getSum());
                        latch.countDown();
                    }

                    @Override
                    public void onFailure(RemoteException e) {
                        throw new RuntimeException(e);
                    }
                });

                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                // Regression test for issue 122.
                request.setA(3);
                request.setB(3);
                serviceClient.call(request, new ServiceResponseListener<AddTwoIntsResponse>() {
                    @Override
                    public void onSuccess(AddTwoIntsResponse response) {
                        log.info("结果:" + response.getSum());
                        latch.countDown();
                    }

                    @Override
                    public void onFailure(RemoteException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        }, nodeConfiguration);

        boolean await = latch.await(10, TimeUnit.SECONDS);

        try {
            nodeClientExecutor.shutdown();
        } catch (Exception e) {
        }
        try {
            nodeMainExecutor.shutdown();
        } catch (Exception e) {
        }
        rosCore.shutdown();

        System.exit(0);
    }

}
