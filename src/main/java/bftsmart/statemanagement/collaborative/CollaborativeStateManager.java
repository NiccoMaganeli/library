package bftsmart.statemanagement.collaborative;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.IntStream;

import bftsmart.consensus.Consensus;
import bftsmart.consensus.Epoch;
import bftsmart.consensus.messages.ConsensusMessage;
import bftsmart.consensus.messages.MessageFactory;
import bftsmart.reconfiguration.views.View;
import bftsmart.statemanagement.ApplicationState;
import bftsmart.statemanagement.SMMessage;
import bftsmart.statemanagement.StateManager;
import bftsmart.tom.core.DeliveryThread;
import bftsmart.tom.core.TOMLayer;
import bftsmart.tom.leaderchange.CertifiedDecision;
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
            CollaborativeState thisState = (CollaborativeState) dt.getRecoverer().getState(cmsg.getCID(), false);
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

            // Copy the part of state
            for (int i = 0; i < partOfState.length; i++) {
                partOfState[i] = serializedState[init + i];
            }

            // Send state back
            int[] targets = { cmsg.getSender() };

            // Should create new instance to make it more semantic?
            // Probably yes...
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
                CertifiedDecision currentProof = null;

                if (!appStateOnly) {
                    senderRegencies.put(msg.getSender(), msg.getRegency());
                    senderLeaders.put(msg.getSender(), msg.getLeader());
                    senderViews.put(msg.getSender(), msg.getView());
                    senderProofs.put(msg.getSender(), msg.getState().getCertifiedDecision(SVController));
                    if (enoughRegencies(msg.getRegency())) {
                        currentRegency = msg.getRegency();
                    }
                    if (enoughLeaders(msg.getLeader())) {
                        currentLeader = msg.getLeader();
                    }
                    if (enoughViews(msg.getView())) {
                        currentView = msg.getView();
                    }
                    if (enoughProofs(waitingCID, this.tomLayer.getSynchronizer().getLCManager())) {
                        currentProof = msg.getState().getCertifiedDecision(SVController);
                    }

                } else {
                    currentLeader = tomLayer.execManager.getCurrentLeader();
                    currentRegency = tomLayer.getSynchronizer().getLCManager().getLastReg();
                    currentView = SVController.getCurrentView();
                }

                ApplicationState replyState = reply.getState();
                if (replyState instanceof CollaborativeState) {
                    senderStates.put(reply.getSender(), reply.getState());
                }

                // Only when number of replies is good....
                // Check # of correct processes and use that...
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

                    CollaborativeState tempState = (CollaborativeState) reply.getState();

                    // HAVE TO REVIEW THIS PARAMETERS
                    CollaborativeState newState = new CollaborativeState(fullState, reply.getCID(),
                            tempState.getMessageBatches(), tempState.getLastCheckpointCID(),
                            SVController.getStaticConf().getProcessId());

                    // Set state locally
                    state = newState;
                    if (stateTimer != null)
                        stateTimer.cancel();

                    if (currentRegency > -1 && currentLeader > -1 && currentView != null
                            && (!isBFT || currentProof != null || appStateOnly)) {
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

                                byte[] hash = tomLayer.computeHash(currentProof.getDecision());
                                e.propValueHash = hash;
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
                        if (currentRegency > 0) {
                            tomLayer.getSynchronizer().removeSTOPretransmissions(currentRegency - 1);
                        }
                        // if (currentRegency > 0)
                        // tomLayer.requestsTimer.setTimeout(tomLayer.requestsTimer.getTimeout() *
                        // (currentRegency * 2));

                        dt.pauseDecisionDelivery();
                        waitingCID = -1;
                        dt.update(state);

                        if (!appStateOnly && execManager.stopped()) {
                            Queue<ConsensusMessage> stoppedMsgs = execManager.getStoppedMsgs();
                            for (ConsensusMessage stopped : stoppedMsgs) {
                                if (stopped.getNumber() > state.getLastCID() /* msg.getCID() */) {
                                    execManager.addOutOfContextMessage(stopped);
                                }
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
                    } else if ((SVController.getCurrentViewN() / 2) < getReplies()) {
                        waitingCID = -1;
                        reset();

                        if (stateTimer != null) {
                            stateTimer.cancel();
                        }

                        if (appStateOnly) {
                            requestState();
                        }
                    } else if (state == null) {
                        logger.debug(
                                "The replica from which I expected the state, sent one which doesn't match the hash of the others, or it never sent it at all");

                        reset();
                        requestState();

                        if (stateTimer != null) {
                            stateTimer.cancel();
                        }
                    } else if ((SVController.getCurrentViewN() - SVController.getCurrentViewF()) <= getReplies()) {

                        logger.debug("Could not obtain the state, retrying");
                        reset();
                        if (stateTimer != null) {
                            stateTimer.cancel();
                        }
                        waitingCID = -1;
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
