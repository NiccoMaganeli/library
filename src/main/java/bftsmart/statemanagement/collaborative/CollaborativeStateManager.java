package bftsmart.statemanagement.collaborative;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.IntStream;

import bftsmart.consensus.messages.ConsensusMessage;
import bftsmart.reconfiguration.views.View;
import bftsmart.statemanagement.ApplicationState;
import bftsmart.statemanagement.SMMessage;
import bftsmart.statemanagement.StateManager;
import bftsmart.tom.util.TOMUtil;

public class CollaborativeStateManager extends StateManager {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private ReentrantLock lockTimer = new ReentrantLock();
    private Timer stateTimer = null;
    private final static long INIT_TIMEOUT = 40000;
    private long timeout = INIT_TIMEOUT;

    @Override
    protected void requestState() {
        if (tomLayer.requestsTimer != null)
            tomLayer.requestsTimer.clearAll();

        int myId = SVController.getStaticConf().getProcessId();
        SMMessage cmsg = new CollaborativeSMMessage(myId, waitingCID, TOMUtil.SM_REQUEST, null, null,
                -1, -1);
        tomLayer.getCommunication().send(SVController.getCurrentViewOtherAcceptors(), cmsg);

        logger.info("Replica requesting state: " + myId + " and waiting for state up to CID " + waitingCID);

        TimerTask stateTask = new TimerTask() {
            public void run() {
                logger.info("Timeout to retrieve state!");
                CollaborativeSMMessage cmsg = new CollaborativeSMMessage(-1, waitingCID, TOMUtil.TRIGGER_SM_LOCALLY,
                        null, null, -1, -1);
                triggerTimeout(cmsg);
            }
        };

        stateTimer = new Timer("state timer");
        timeout = timeout * 2;
        stateTimer.schedule(stateTask, timeout);
    }

    @Override
    public void stateTimeout() {
        lockTimer.lock();

        logger.debug("Is there a way to find which replica timed-out?");

        if (stateTimer != null)
            stateTimer.cancel();

        reset();
        requestState(); // Requesting again...

        lockTimer.unlock();
    }

    @Override
    public void SMRequestDeliver(SMMessage msg, boolean isBFT) {
        if (SVController.getStaticConf().isStateTransferEnabled() && dt.getRecoverer() != null) {
            CollaborativeSMMessage cmsg = (CollaborativeSMMessage) msg;

            // Decide which part of state to send
            int[] allProcesses = SVController.getCurrentViewAcceptors();
            int[] parts = IntStream.range(0, allProcesses.length).filter(n -> n != cmsg.getSender()).toArray();

            // Generate that part
            ApplicationState thisState = dt.getRecoverer().getState(cmsg.getCID(), false); // no use for sendState
            byte[] serializedState = thisState.getSerializedState();
            // Just for safety
            byte[] partOfState = new byte[serializedState.length];

            // Split as evenly as possible
            int[] stateSize = getStateSize(parts.length, serializedState.length);

            int myId = SVController.getStaticConf().getProcessId();
            int init = 0;
            for (int i = 0; i < parts.length; i++) {
                if (parts[i] == myId) {
                    partOfState = new byte[stateSize[i]];
                    break;
                } else {
                    init += stateSize[i];
                }
            }

            for (int i = 0; i < partOfState.length; i++) {
                partOfState[i] = serializedState[init + i];
            }

            // Send state back
            int[] targets = { cmsg.getSender() };

            // SET PART OF STATE - PROBABLY SHOULD SEPARATE THIS
            thisState.setSerializedState(partOfState);

            CollaborativeSMMessage rmsg = new CollaborativeSMMessage(myId, msg.getCID(), TOMUtil.SM_REPLY, thisState,
                    SVController.getCurrentView(), tomLayer.getSynchronizer().getLCManager().getLastReg(),
                    tomLayer.execManager.getCurrentLeader());

            logger.info("Sendind state from " + myId + " back with " + partOfState.length + " bytes long");
            tomLayer.getCommunication().send(targets, rmsg);
            logger.info("Sent!");
        }

    }

    @Override
    public void SMReplyDeliver(SMMessage msg, boolean isBFT) {
        lockTimer.lock();
        CollaborativeSMMessage reply = (CollaborativeSMMessage) msg;
        if (SVController.getStaticConf().isStateTransferEnabled()) {
            logger.info("Received a state reply for CID " + reply.getCID() + " from " + reply.getSender());
            if (waitingCID != -1 && reply.getCID() == waitingCID) {
                int currentRegency = -1;
                int currentLeader = -1;
                View currentView = null;

                if (!appStateOnly) {
                    senderRegencies.put(reply.getSender(), reply.getRegency());
                    senderLeaders.put(reply.getSender(), reply.getLeader());
                    senderViews.put(reply.getSender(), reply.getView());
                    // senderProofs.put(msg.getSender(),
                    // msg.getState().getCertifiedDecision(SVController));
                    if (enoughRegencies(reply.getRegency())) {
                        currentRegency = reply.getRegency();
                    }
                    if (enoughLeaders(reply.getLeader())) {
                        currentLeader = reply.getLeader();
                    }
                    if (enoughViews(reply.getView())) {
                        currentView = reply.getView();
                        if (!currentView.isMember(SVController.getStaticConf()
                                .getProcessId())) {
                            logger.warn("Not a member!");
                        }
                    }
                    // if (enoughProofs(waitingCID, this.tomLayer.getSynchronizer().getLCManager()))
                    // currentProof = msg.getState().getCertifiedDecision(SVController);

                } else {
                    currentLeader = tomLayer.execManager.getCurrentLeader();
                    currentRegency = tomLayer.getSynchronizer().getLCManager().getLastReg();
                    currentView = SVController.getCurrentView();
                }

                senderStates.put(reply.getSender(), reply.getState());

                if (senderStates.size() == SVController.getStaticConf().getN()) {
                    // Sort parts
                    TreeMap<Integer, ApplicationState> sorted = new TreeMap<>(senderStates);
                    ApplicationState[] parts = (ApplicationState[]) sorted.values().toArray();

                    int fullSize = 0;
                    for (ApplicationState part : parts)
                        fullSize += part.getSerializedState().length;

                    byte[] fullState = new byte[fullSize];

                    int lastPart = 0;
                    for (ApplicationState part : parts) {
                        byte[] partState = part.getSerializedState();
                        System.arraycopy(partState, 0, fullState, lastPart, partState.length);
                        lastPart += partState.length;
                    }

                    // HAVE TO CREATE NEW STATE AND STOP DEPENDING ON APPLICATIONSTATE
                    ApplicationState oneState = parts[0];
                    oneState.setSerializedState(fullState);

                    // Set state locally
                    state = oneState;
                    if (stateTimer != null)
                        stateTimer.cancel();

                    // BOILERPLATE TO KEEP BFT RUNNING ?
                    if (currentRegency > -1 && currentLeader > -1
                            && currentView != null
                            && (!isBFT || appStateOnly)) {
                        logger.info("---- RECEIVED VALID STATE ----");

                        logger.debug("The state of those replies is good!");
                        logger.debug("CID State requested: " + reply.getCID());

                        tomLayer.getSynchronizer().getLCManager().setLastReg(currentRegency);
                        tomLayer.getSynchronizer().getLCManager().setNextReg(currentRegency);
                        tomLayer.getSynchronizer().getLCManager().setNewLeader(currentLeader);

                        tomLayer.execManager.setNewLeader(currentLeader);

                        if (currentRegency > 0) {
                            tomLayer.getSynchronizer().removeSTOPretransmissions(currentRegency - 1);
                        }

                        logger.debug("trying to acquire deliverlock");
                        dt.pauseDecisionDelivery();
                        logger.debug("acquired");

                        // this makes the isRetrievingState() evaluates to false
                        waitingCID = -1;
                        dt.update(state); // CALLS SET STATE PROPERLY

                        // Deal with stopped messages that may come from
                        // synchronization phase
                        if (!appStateOnly && execManager.stopped()) {
                            Queue<ConsensusMessage> stoppedMsgs = execManager.getStoppedMsgs();
                            for (ConsensusMessage stopped : stoppedMsgs) {
                                if (stopped.getNumber() > state.getLastCID()) {
                                    execManager.addOutOfContextMessage(stopped);
                                }
                            }
                            execManager.clearStopped();
                            execManager.restart();
                        }

                        logger.info("Processing out of context messages");
                        tomLayer.processOutOfContext();

                        if (SVController.getCurrentViewId() != currentView.getId()) {
                            logger.info("Installing current view!");
                            SVController.reconfigureTo(currentView);
                        }

                        isInitializing = false;

                        dt.canDeliver();
                        dt.resumeDecisionDelivery();

                        reset();

                        logger.info("I updated the state!");

                        tomLayer.requestsTimer.Enabled(true);
                        tomLayer.requestsTimer.startTimer();
                        if (stateTimer != null) {
                            stateTimer.cancel();
                        }

                        if (appStateOnly) {
                            appStateOnly = false;
                            tomLayer.getSynchronizer().resumeLC();
                        }
                    } else {
                        logger.info("Still receiving state...");
                    }
                }
            }
        }
        lockTimer.unlock();
    }

    private int[] getStateSize(int parts, int stateSize) {

        int localRange = (stateSize - 1) / parts + 1;
        int spare = localRange * parts - stateSize;
        int currentIndex = 0;
        int[] tasksSize = new int[parts];

        for (int i = 0; i < parts; i++) {
            final int min = currentIndex;
            final int max = min + localRange - (i < spare ? 1 : 0);
            tasksSize[i] = max - min;
            currentIndex = max;
        }

        return tasksSize;

    }

}
