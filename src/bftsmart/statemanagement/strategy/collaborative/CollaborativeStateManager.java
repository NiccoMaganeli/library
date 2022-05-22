package bftsmart.statemanagement.strategy.collaborative;

import bftsmart.consensus.Consensus;
import bftsmart.consensus.Epoch;
import bftsmart.consensus.messages.ConsensusMessage;
import bftsmart.consensus.messages.MessageFactory;
import bftsmart.reconfiguration.views.View;
import bftsmart.statemanagement.ApplicationState;
import bftsmart.statemanagement.SMMessage;
import bftsmart.statemanagement.strategy.BaseStateManager;
import bftsmart.tom.core.DeliveryThread;
import bftsmart.tom.core.ExecutionManager;
import bftsmart.tom.core.TOMLayer;
import bftsmart.tom.leaderchange.CertifiedDecision;
import bftsmart.tom.util.TOMUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.IntStream;

/**
 * @author Niccolas Maganeli
 */
public class CollaborativeStateManager extends BaseStateManager {

    private final static long INIT_TIMEOUT = 10 * 1000;
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final ReentrantLock lockTimer = new ReentrantLock();
    private Timer stateTimer = null;
    private long timeout = INIT_TIMEOUT;

    private ExecutionManager execManager;

    @Override
    public void init(TOMLayer tomLayer, DeliveryThread dt) {
        SVController = tomLayer.controller;

        this.tomLayer = tomLayer;
        this.dt = dt;
        this.execManager = tomLayer.execManager;

        state = null;
        lastCID = -1;
        waitingCID = -1;

        appStateOnly = false;
    }

    @Override
    protected void requestState() {
        requestState(SVController.getCurrentViewOtherAcceptors(), null);
    }

    private void requestState(int[] replicasWhichResponded, int[] whichReplicas) {
        if (tomLayer.requestsTimer != null) tomLayer.requestsTimer.clearAll();

        SMMessage smsg = new CollaborativeSMMessage(SVController.getStaticConf().getProcessId(), waitingCID,
                TOMUtil.SM_REQUEST, null, null, -1, -1, whichReplicas);
        tomLayer.getCommunication().send(replicasWhichResponded, smsg);

        logger.info("I just sent a request to the other replicas for the state up to CID " + waitingCID);

        TimerTask stateTask = new TimerTask() {
            public void run() {
                logger.info("Timeout to retrieve state");
                int[] myself = new int[1];
                myself[0] = SVController.getStaticConf().getProcessId();
                tomLayer.getCommunication().send(myself, new CollaborativeSMMessage(-1, waitingCID,
                        TOMUtil.TRIGGER_SM_LOCALLY, null, null, -1, -1, null));
            }
        };

        stateTimer = new Timer("state timer");
        timeout = timeout * 2;
        stateTimer.schedule(stateTask, timeout);
    }

    @Override
    public void stateTimeout() {
        lockTimer.lock();

        logger.info("Request for state timed out. Requesting again!");

        if (stateTimer != null) stateTimer.cancel();

        // Check if all replicas failed
        int[] currentViewAcceptors = SVController.getCurrentViewAcceptors();
        int myId = SVController.getStaticConf().getProcessId();
        int[] replicasMissingState =
                Arrays.stream(currentViewAcceptors).filter(st -> !senderStates.containsKey(st) && st != myId).toArray();

        if (replicasMissingState.length == currentViewAcceptors.length) {
            logger.info("No replicas sent the state");
            reset();
            requestState();
        } else {
            logger.info("At least some replicas sent state");
            int[] replicasWhichResponded = senderStates.keySet().stream().mapToInt(Integer::intValue).toArray();

            logger.info("Replicas responded: " + Arrays.toString(replicasWhichResponded) + " Replicas missing state: "
                    + Arrays.toString(replicasMissingState));

            requestState(replicasWhichResponded, replicasMissingState);
        }

        lockTimer.unlock();
    }

    @Override
    public void SMRequestDeliver(SMMessage msg, boolean isBFT) {
        if (SVController.getStaticConf().isStateTransferEnabled() && dt.getRecoverer() != null) {
            CollaborativeSMMessage stdMsg = (CollaborativeSMMessage) msg;
            boolean sendState = true;

            // Decide which part of state to send
            int[] allProcesses = SVController.getCurrentViewAcceptors();
            int[] parts = IntStream.range(0, allProcesses.length).filter(n -> n != stdMsg.getSender()).toArray();

            logger.info("All Procs: " + Arrays.toString(allProcesses) + " Parts: " + Arrays.toString(parts));

            int myId = SVController.getStaticConf().getProcessId();
            int[] targets = {msg.getSender()};
            boolean sendVoidState;

            ApplicationState thisState = dt.getRecoverer().getState(msg.getCID(), sendState);
            if (thisState == null) {
                logger.warn("For some reason, I am sending a void state");
                thisState = dt.getRecoverer().getState(-1, sendState);
                sendVoidState = true;
            } else {
                byte[] serializedState = thisState.getSerializedState();
                if (serializedState == null) {
                    logger.warn("Serialized state is null, sending void state");
                    thisState = dt.getRecoverer().getState(-1, sendState);
                    sendVoidState = true;
                } else {
                    sendVoidState = false;
                    logger.info("Total state size: " + serializedState.length + " bytes");
                    // Just for safety
                    byte[] partOfState = new byte[serializedState.length];

                    // Split as evenly as possible
                    int[] stateSize = getStateSize(parts.length, serializedState.length);

                    // Decide if I'll have to send my part or others parts
                    int[] whichPart = stdMsg.getWhichPart();
                    if (whichPart == null) {
                        whichPart = new int[]{myId};
                    }

                    // Mount as many states as I should mount.
                    for (int statePart : whichPart) {
                        logger.info("Mounting state for replica " + statePart);
                        int init = 0;
                        for (int i = 0; i < parts.length; i++) {
                            if (parts[i] == statePart) {
                                partOfState = new byte[stateSize[i]];
                                break;
                            } else {
                                init += stateSize[i];
                            }
                        }
                        // Copy the part of state
                        System.arraycopy(serializedState, init, partOfState, 0, partOfState.length);
                        thisState.setSerializedState(partOfState);

                        // Am I building mine or others parts?
                        int[] builtState;
                        if (myId == statePart) {
                            builtState = null;
                        } else {
                            builtState = new int[]{statePart};
                        }


                        SMMessage smsg = new CollaborativeSMMessage(myId, msg.getCID(), TOMUtil.SM_REPLY, thisState,
                                SVController.getCurrentView(), tomLayer.getSynchronizer().getLCManager().getLastReg()
                                , tomLayer.execManager.getCurrentLeader(), builtState);
                        logger.info("Sending state part " + statePart + " with " + partOfState.length + " bytes long "
                                + "to " + Arrays.toString(targets));
                        tomLayer.getCommunication().send(targets, smsg);
                        logger.info("Sent");
                    }
                }
            }

            if (sendVoidState) {
                SMMessage smsg = new CollaborativeSMMessage(SVController.getStaticConf().getProcessId(), msg.getCID()
                        , TOMUtil.SM_REPLY, thisState, SVController.getCurrentView(),
                        tomLayer.getSynchronizer().getLCManager().getLastReg(),
                        tomLayer.execManager.getCurrentLeader(), null);
                tomLayer.getCommunication().send(targets, smsg);
            }
        }
    }

    @Override
    public void SMReplyDeliver(SMMessage msg, boolean isBFT) {
        lockTimer.lock();
        if (SVController.getStaticConf().isStateTransferEnabled()) {
            if (waitingCID != -1 && msg.getCID() == waitingCID) {
                int currentRegency = -1;
                int currentLeader = -1;
                View currentView = null;
                CertifiedDecision currentProof = null;

                if (!appStateOnly) {
                    senderRegencies.put(msg.getSender(), msg.getRegency());
                    senderLeaders.put(msg.getSender(), msg.getLeader());
                    senderViews.put(msg.getSender(), msg.getView());
                    senderProofs.put(msg.getSender(), msg.getState().getCertifiedDecision(SVController));
                    if (enoughRegencies(msg.getRegency())) currentRegency = msg.getRegency();
                    if (enoughLeaders(msg.getLeader())) currentLeader = msg.getLeader();
                    if (enoughViews(msg.getView())) currentView = msg.getView();
                    if (enoughProofs(waitingCID, this.tomLayer.getSynchronizer().getLCManager()))
                        currentProof = msg.getState().getCertifiedDecision(SVController);

                } else {
                    currentLeader = tomLayer.execManager.getCurrentLeader();
                    currentRegency = tomLayer.getSynchronizer().getLCManager().getLastReg();
                    currentView = SVController.getCurrentView();
                }

                CollaborativeSMMessage cmsg = (CollaborativeSMMessage) msg;
                int sender = msg.getSender();
                ApplicationState replyState = msg.getState();
                logger.info("Received part of state from " + sender);

                int[] builtState = cmsg.getWhichPart();
                int whichId;

                if (builtState == null) {
                    whichId = sender;
                } else {
                    whichId = builtState[0];
                }

                if (!senderStates.containsKey(whichId) && replyState != null) senderStates.put(whichId, replyState);

                boolean numberOfRepliesMatchView =
                        senderStates.size() == SVController.getCurrentViewOtherAcceptors().length;
                boolean allSerializedIsGood =
                        senderStates.values().stream().allMatch(st -> st.getSerializedState() != null);

                if (numberOfRepliesMatchView && allSerializedIsGood) {
                    logger.info("Sorting parts");
                    TreeMap<Integer, ApplicationState> sorted = new TreeMap<>(senderStates);
                    ApplicationState[] parts = sorted.values().toArray(new ApplicationState[0]);

                    int fullSize = Arrays.stream(parts).mapToInt(st -> st.getSerializedState().length).sum();
                    byte[] fullState = new byte[fullSize];

                    logger.info("Remounting full state with parts");
                    int lastPart = 0;
                    for (ApplicationState part : parts) {
                        byte[] partState = part.getSerializedState();
                        System.arraycopy(partState, 0, fullState, lastPart, partState.length);
                        lastPart += partState.length;
                    }

                    ApplicationState tempState = msg.getState();
                    tempState.setSerializedState(fullState);

                    logger.info("State set");
                    // Set state locally
                    state = tempState;
                    if (stateTimer != null) stateTimer.cancel();
                }

                logger.info("Verifying more than F replies");
                if (enoughReplies()) {
                    logger.info("More than F confirmed");

                    if (state != null && currentRegency > -1 && currentLeader > -1 && currentView != null && (!isBFT || currentProof != null || appStateOnly)) {
                        logger.info("Received state. Will install it");

                        tomLayer.getSynchronizer().getLCManager().setLastReg(currentRegency);
                        tomLayer.getSynchronizer().getLCManager().setNextReg(currentRegency);
                        tomLayer.getSynchronizer().getLCManager().setNewLeader(currentLeader);
                        tomLayer.execManager.setNewLeader(currentLeader);

                        if (currentProof != null && !appStateOnly) {

                            logger.debug("Installing proof for consensus " + waitingCID);

                            Consensus cons = execManager.getConsensus(waitingCID);
                            Epoch e = null;

                            for (ConsensusMessage cm : currentProof.getConsMessages()) {

                                e = cons.getEpoch(cm.getEpoch(), true, SVController);
                                if (e.getTimestamp() != cm.getEpoch()) {

                                    logger.warn("Strange... proof contains messages from more than just one epoch");
                                    e = cons.getEpoch(cm.getEpoch(), true, SVController);
                                }
                                e.addToProof(cm);

                                if (cm.getType() == MessageFactory.ACCEPT) {
                                    e.setAccept(cm.getSender(), cm.getValue());
                                } else if (cm.getType() == MessageFactory.WRITE) {
                                    e.setWrite(cm.getSender(), cm.getValue());
                                }

                            }

                            if (e != null) {

                                e.propValueHash = tomLayer.computeHash(currentProof.getDecision());
                                e.propValue = currentProof.getDecision();
                                e.deserializedPropValue = tomLayer.checkProposedValue(currentProof.getDecision(),
                                        false);
                                cons.decided(e, false);

                                logger.info("Successfully installed proof for consensus " + waitingCID);

                            } else {
                                logger.error("Failed to install proof for consensus " + waitingCID);

                            }

                        }

                        // I might have timed out before invoking the state transfer, so
                        // stop my re-transmission of STOP messages for all regencies up to the current
                        // one
                        if (currentRegency > 0)
                            tomLayer.getSynchronizer().removeSTOPretransmissions(currentRegency - 1);

                        dt.deliverLock();
                        waitingCID = -1;
                        dt.update(state);

                        if (!appStateOnly && execManager.stopped()) {
                            Queue<ConsensusMessage> stoppedMsgs = execManager.getStoppedMsgs();
                            for (ConsensusMessage stopped : stoppedMsgs) {
                                if (stopped.getNumber() > state.getLastCID() /* msg.getCID() */)
                                    execManager.addOutOfContextMessage(stopped);
                            }
                            execManager.clearStopped();
                            execManager.restart();
                        }

                        tomLayer.processOutOfContext();

                        if (SVController.getCurrentViewId() != currentView.getId()) {
                            logger.info("Installing current view!");
                            SVController.reconfigureTo(currentView);
                        }

                        isInitializing = false;

                        dt.canDeliver();
                        dt.deliverUnlock();

                        reset();

                        logger.info("I updated the state!");

                        tomLayer.requestsTimer.Enabled(true);
                        tomLayer.requestsTimer.startTimer();
                        if (stateTimer != null) stateTimer.cancel();

                        if (appStateOnly) {
                            appStateOnly = false;
                            tomLayer.getSynchronizer().resumeLC();
                        }
                    } else if (numberOfRepliesMatchView && !allSerializedIsGood) {
                        logger.info("Some states are broke. Reset and request everything");

                        if (stateTimer != null) stateTimer.cancel();

                        reset();
                        requestState();


                    } else {
                        logger.debug("State transfer not yet finished");
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
