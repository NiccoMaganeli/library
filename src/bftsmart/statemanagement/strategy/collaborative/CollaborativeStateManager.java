package bftsmart.statemanagement.collaborative;

/**
Copyright (c) 2007-2013 Alysson Bessani, Eduardo Alchieri, Paulo Sousa, and the authors indicated in the @author tags

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
// package bftsmart.statemanagement.strategy;

import java.util.ArrayList;
import java.util.TreeMap;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.ReentrantLock;
import java.util.Random;
import java.util.stream.IntStream;

import bftsmart.tom.core.ExecutionManager;
import bftsmart.consensus.messages.ConsensusMessage;
import bftsmart.consensus.messages.MessageFactory;
import bftsmart.reconfiguration.views.View;
import bftsmart.statemanagement.ApplicationState;
import bftsmart.statemanagement.SMMessage;
import bftsmart.statemanagement.strategy.BaseStateManager;
import bftsmart.statemanagement.strategy.StandardSMMessage;
import bftsmart.tom.core.DeliveryThread;
import bftsmart.tom.core.TOMLayer;
import bftsmart.tom.util.TOMUtil;
import bftsmart.consensus.Consensus;
import bftsmart.consensus.Epoch;
import bftsmart.tom.leaderchange.CertifiedDecision;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author Marcel Santos
 *
 */
public class CollaborativeStateManager extends BaseStateManager {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private ReentrantLock lockTimer = new ReentrantLock();
    private Timer stateTimer = null;
    private final static long INIT_TIMEOUT = 40000;
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
        if (tomLayer.requestsTimer != null)
            tomLayer.requestsTimer.clearAll();

        SMMessage smsg = new CollaborativeSMMessage(SVController.getStaticConf().getProcessId(),
                waitingCID, TOMUtil.SM_REQUEST, null, null, -1, -1);
        tomLayer.getCommunication().send(SVController.getCurrentViewOtherAcceptors(), smsg);

        logger.info("I just sent a request to the other replicas for the state up to CID " + waitingCID);

        TimerTask stateTask = new TimerTask() {
            public void run() {
                logger.info("Timeout to retrieve state");
                int[] myself = new int[1];
                myself[0] = SVController.getStaticConf().getProcessId();
                tomLayer.getCommunication().send(myself,
                        new CollaborativeSMMessage(-1, waitingCID, TOMUtil.TRIGGER_SM_LOCALLY, null, null, -1, -1));
            }
        };

        stateTimer = new Timer("state timer");
        timeout = timeout * 2;
        stateTimer.schedule(stateTask, timeout);
    }

    @Override
    public void stateTimeout() {
        lockTimer.lock();
        logger.debug("Timeout for the replica that was supposed to send the complete state. Changing desired replica.");
        if (stateTimer != null)
            stateTimer.cancel();

        reset();
        requestState();
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
            int myId = SVController.getStaticConf().getProcessId();

            ApplicationState thisState = dt.getRecoverer().getState(msg.getCID(), sendState);
            if (thisState == null) {
                logger.warn("For some reason, I am sending a void state");
                thisState = dt.getRecoverer().getState(-1, sendState);
            } else {
                byte[] serializedState = thisState.getSerializedState();
                if (serializedState == null) {
                    logger.info("Serialized state is null");
                } else {
                    logger.info("Total state size: " + serializedState.length + " bytes");
                    // Just for safety
                    byte[] partOfState = new byte[serializedState.length];
    
                    // Split as evenly as possible
                    int[] stateSize = getStateSize(parts.length, serializedState.length);
    
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
    
                    thisState.setSerializedState(partOfState);
                    logger.info("Sending state from replica " + myId + " back with " + partOfState.length
                            + " bytes long");
                }
            }

            int[] targets = { msg.getSender() };
            SMMessage smsg = new StandardSMMessage(SVController.getStaticConf().getProcessId(),
                    msg.getCID(), TOMUtil.SM_REPLY, -1, thisState, SVController.getCurrentView(),
                    tomLayer.getSynchronizer().getLCManager().getLastReg(), tomLayer.execManager.getCurrentLeader());

            logger.info("Sending state...");
            tomLayer.getCommunication().send(targets, smsg);
            logger.info("Sent");
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
                    if (enoughRegencies(msg.getRegency()))
                        currentRegency = msg.getRegency();
                    if (enoughLeaders(msg.getLeader()))
                        currentLeader = msg.getLeader();
                    if (enoughViews(msg.getView()))
                        currentView = msg.getView();
                    if (enoughProofs(waitingCID, this.tomLayer.getSynchronizer().getLCManager()))
                        currentProof = msg.getState().getCertifiedDecision(SVController);

                } else {
                    currentLeader = tomLayer.execManager.getCurrentLeader();
                    currentRegency = tomLayer.getSynchronizer().getLCManager().getLastReg();
                    currentView = SVController.getCurrentView();
                }

                logger.info("Replica " + SVController.getStaticConf().getProcessId() + " received part of state from " + msg.getSender());
                logger.info("Part size: " + msg.getState().getSerializedState().length);
                ApplicationState replyState = msg.getState();
                senderStates.put(msg.getSender(), msg.getState());

                // Only when number of replies is good....
                // Check # of correct processes and use that...

                // We have all states with information?
                boolean allSerializedIsValid = senderStates.values().stream().allMatch(state -> state.getSerializedState() != null);
                boolean numberOfRepliesMatchView = senderStates.size() == SVController.getCurrentViewOtherAcceptors().length;

                if (numberOfRepliesMatchView && allSerializedIsValid) {
                    // Sort parts
                    logger.info("Sorting parts");
                    TreeMap<Integer, ApplicationState> sorted = new TreeMap<>(senderStates);
                    ApplicationState[] parts = (ApplicationState[]) sorted.values().toArray(new ApplicationState[sorted.size()]);

                    logger.info("Calculating full size of state");
                    int fullSize = 0;
                    for (ApplicationState part : parts) 
                        fullSize += part.getSerializedState().length;
                    

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
                    if (stateTimer != null)
                        stateTimer.cancel();
                }

                logger.debug("Verifying more than F replies");
                if (enoughReplies()) {
                    logger.debug("More than F confirmed");

                    if (state != null && currentRegency > -1 &&
                            currentLeader > -1 && currentView != null
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
                                }

                                else if (cm.getType() == MessageFactory.WRITE) {
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
                        if (currentRegency > 0)
                            tomLayer.getSynchronizer().removeSTOPretransmissions(currentRegency - 1);
                        // if (currentRegency > 0)
                        // tomLayer.requestsTimer.setTimeout(tomLayer.requestsTimer.getTimeout() *
                        // (currentRegency * 2));

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
                        if (stateTimer != null)
                            stateTimer.cancel();

                        if (appStateOnly) {
                            appStateOnly = false;
                            tomLayer.getSynchronizer().resumeLC();
                        }
                    } else if (numberOfRepliesMatchView && !allSerializedIsValid){
                        logger.info("Already have all replies, but some are missing. Requesting again");
                        reset();
                        requestState();
                    } else if (!allSerializedIsValid && (SVController.getCurrentViewN() / 2) < getReplies()) {
                        waitingCID = -1;
                        reset();

                        if (stateTimer != null)
                            stateTimer.cancel();

                        if (appStateOnly) {
                            requestState();
                        }
                    } else if ((SVController.getCurrentViewN() - SVController.getCurrentViewF()) <= getReplies()) {

                        logger.debug("Could not obtain the state, retrying");
                        reset();
                        if (stateTimer != null)
                            stateTimer.cancel();
                        waitingCID = -1;
                        //requestState();
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

// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;

// import java.util.Queue;
// import java.util.Timer;
// import java.util.TimerTask;
// import java.util.TreeMap;
// import java.util.concurrent.locks.ReentrantLock;
// import java.util.stream.IntStream;

// import bftsmart.consensus.Consensus;
// import bftsmart.consensus.Epoch;
// import bftsmart.consensus.messages.ConsensusMessage;
// import bftsmart.consensus.messages.MessageFactory;
// import bftsmart.reconfiguration.views.View;
// import bftsmart.statemanagement.ApplicationState;
// import bftsmart.statemanagement.SMMessage;
// import bftsmart.statemanagement.strategy.BaseStateManager;
// import bftsmart.tom.core.DeliveryThread;
// import bftsmart.tom.core.ExecutionManager;
// import bftsmart.tom.core.TOMLayer;
// import bftsmart.tom.leaderchange.CertifiedDecision;
// import bftsmart.tom.util.TOMUtil;

// public class CollaborativeStateManager extends BaseStateManager {

// private Logger logger = LoggerFactory.getLogger(this.getClass());

// private ReentrantLock lockTimer = new ReentrantLock();
// private Timer stateTimer = null;
// private final static long INIT_TIMEOUT = 40000;
// private long timeout = INIT_TIMEOUT;

// private ExecutionManager execManager;

// @Override
// public void init(TOMLayer tomLayer, DeliveryThread dt) {
// SVController = tomLayer.controller;
// this.tomLayer = tomLayer;
// this.dt = dt;
// this.execManager = tomLayer.execManager;

// state = null;
// lastCID = 1;
// waitingCID = -1;

// appStateOnly = false;
// }

// @Override
// protected void requestState() {
// if (tomLayer.requestsTimer != null)
// tomLayer.requestsTimer.clearAll();

// int myId = SVController.getStaticConf().getProcessId();
// SMMessage cmsg = new CollaborativeSMMessage(myId, waitingCID,
// TOMUtil.SM_REQUEST, null, null,
// -1, -1);
// tomLayer.getCommunication().send(SVController.getCurrentViewOtherAcceptors(),
// cmsg);

// logger.info("Replica requesting state: " + myId + " and waiting for state up
// to CID " + waitingCID);

// TimerTask stateTask = new TimerTask() {
// public void run() {
// logger.info("Timeout to retrieve state!");
// CollaborativeSMMessage cmsg = new CollaborativeSMMessage(-1, waitingCID,
// TOMUtil.TRIGGER_SM_LOCALLY,
// null, null, -1, -1);
// tomLayer.getCommunication().send(new int[1], cmsg);
// }
// };

// stateTimer = new Timer("state timer");
// timeout = timeout * 2;
// stateTimer.schedule(stateTask, timeout);
// }

// @Override
// public void stateTimeout() {
// lockTimer.lock();

// logger.debug("Is there a way to find which replica timed-out?");

// if (stateTimer != null)
// stateTimer.cancel();

// reset();
// requestState(); // Requesting again...

// lockTimer.unlock();
// }

// @Override
// public void SMRequestDeliver(SMMessage msg, boolean isBFT) {
// if (SVController.getStaticConf().isStateTransferEnabled() &&
// dt.getRecoverer() != null) {
// CollaborativeSMMessage cmsg = (CollaborativeSMMessage) msg;
// boolean sendState = true; // Every replica sends a part of state

// // Decide which part of state to send
// int[] allProcesses = SVController.getCurrentViewAcceptors();
// int[] parts = IntStream.range(0, allProcesses.length).filter(n -> n !=
// cmsg.getSender()).toArray();

// // Generate that part
// ApplicationState thisState = dt.getRecoverer().getState(cmsg.getCID(),
// sendState);
// int myId = SVController.getStaticConf().getProcessId();

// if (thisState == null) {
// logger.info("Void state detected. Maybe beginning of life?");
// thisState = dt.getRecoverer().getState(-1, sendState);
// } else {
// byte[] serializedState = thisState.getSerializedState();
// if (serializedState == null) { // This shouldn't be null!!
// thisState = dt.getRecoverer().getState(-1, sendState);
// } else {
// // Just for safety
// byte[] partOfState = new byte[serializedState.length];

// // Split as evenly as possible
// int[] stateSize = getStateSize(parts.length, serializedState.length);

// int init = 0;
// for (int i = 0; i < parts.length; i++) {
// if (parts[i] == myId) {
// partOfState = new byte[stateSize[i]];
// break;
// } else {
// init += stateSize[i];
// }
// }

// // Copy the part of state
// for (int i = 0; i < partOfState.length; i++) {
// partOfState[i] = serializedState[init + i];
// }

// // Should create new instance to make it more semantic?
// // Probably yes...ß
// thisState.setSerializedState(partOfState);
// logger.info("Sending state from " + myId + " back with " + partOfState.length
// + " bytes long");
// }
// }

// // Send state back
// int[] targets = { cmsg.getSender() };
// CollaborativeSMMessage rmsg = new CollaborativeSMMessage(myId, msg.getCID(),
// TOMUtil.SM_REPLY, thisState,
// SVController.getCurrentView(),
// tomLayer.getSynchronizer().getLCManager().getLastReg(),
// tomLayer.execManager.getCurrentLeader());

// tomLayer.getCommunication().send(targets, rmsg);
// logger.info("Sent!");
// }

// }

// @Override
// public void SMReplyDeliver(SMMessage msg, boolean isBFT) {
// lockTimer.lock();
// CollaborativeSMMessage reply = (CollaborativeSMMessage) msg;
// if (SVController.getStaticConf().isStateTransferEnabled()) {
// logger.info("Received a state reply for CID " + reply.getCID() + " from " +
// reply.getSender());
// if (waitingCID != -1 && reply.getCID() == waitingCID) {
// logger.info("CID != -1 && getCID() == waitingCID");
// int currentRegency = -1;
// int currentLeader = -1;
// View currentView = null;
// CertifiedDecision currentProof = null;

// if (!appStateOnly) {
// senderRegencies.put(msg.getSender(), msg.getRegency());
// senderLeaders.put(msg.getSender(), msg.getLeader());
// senderViews.put(msg.getSender(), msg.getView());
// senderProofs.put(msg.getSender(),
// msg.getState().getCertifiedDecision(SVController));
// if (enoughRegencies(msg.getRegency())) {
// currentRegency = msg.getRegency();
// }
// if (enoughLeaders(msg.getLeader())) {
// currentLeader = msg.getLeader();
// }
// if (enoughViews(msg.getView())) {
// currentView = msg.getView();
// }
// if (enoughProofs(waitingCID, this.tomLayer.getSynchronizer().getLCManager()))
// {
// currentProof = msg.getState().getCertifiedDecision(SVController);
// }

// } else {
// currentLeader = tomLayer.execManager.getCurrentLeader();
// currentRegency = tomLayer.getSynchronizer().getLCManager().getLastReg();
// currentView = SVController.getCurrentView();
// }

// ApplicationState replyState = reply.getState();
// senderStates.put(reply.getSender(), reply.getState());

// // Only when number of replies is good....
// // Check # of correct processes and use that...
// if (senderStates.size() == SVController.getStaticConf().getN()) {
// // Sort parts
// TreeMap<Integer, ApplicationState> sorted = new TreeMap<>(senderStates);
// ApplicationState[] parts = (ApplicationState[]) sorted.values().toArray();

// int fullSize = 0;
// for (ApplicationState part : parts)
// fullSize += part.getSerializedState().length;

// byte[] fullState = new byte[fullSize];

// int lastPart = 0;
// for (ApplicationState part : parts) {
// byte[] partState = part.getSerializedState();
// System.arraycopy(partState, 0, fullState, lastPart, partState.length);
// lastPart += partState.length;
// }

// ApplicationState tempState = reply.getState();
// tempState.setSerializedState(fullState);

// // Set state locally
// state = tempState;
// if (stateTimer != null)
// stateTimer.cancel();

// if (currentRegency > -1 && currentLeader > -1 && currentView != null
// && (!isBFT || currentProof != null || appStateOnly)) {
// logger.info("Received state. Will install it");

// tomLayer.getSynchronizer().getLCManager().setLastReg(currentRegency);
// tomLayer.getSynchronizer().getLCManager().setNextReg(currentRegency);
// tomLayer.getSynchronizer().getLCManager().setNewLeader(currentLeader);
// tomLayer.execManager.setNewLeader(currentLeader);

// if (currentProof != null && !appStateOnly) {

// logger.debug("Installing proof for consensus " + waitingCID);

// Consensus cons = execManager.getConsensus(waitingCID);
// Epoch e = null;

// for (ConsensusMessage cm : currentProof.getConsMessages()) {

// e = cons.getEpoch(cm.getEpoch(), true, SVController);
// if (e.getTimestamp() != cm.getEpoch()) {

// logger.warn("Strange... proof contains messages from more than just one
// epoch");
// e = cons.getEpoch(cm.getEpoch(), true, SVController);
// }
// e.addToProof(cm);

// if (cm.getType() == MessageFactory.ACCEPT) {
// e.setAccept(cm.getSender(), cm.getValue());
// } else if (cm.getType() == MessageFactory.WRITE) {
// e.setWrite(cm.getSender(), cm.getValue());
// }

// }

// if (e != null) {

// byte[] hash = tomLayer.computeHash(currentProof.getDecision());
// e.propValueHash = hash;
// e.propValue = currentProof.getDecision();
// e.deserializedPropValue =
// tomLayer.checkProposedValue(currentProof.getDecision(),
// false);
// cons.decided(e, false);

// logger.info("Successfully installed proof for consensus " + waitingCID);

// } else {
// logger.error("Failed to install proof for consensus " + waitingCID);

// }

// }

// // I might have timed out before invoking the state transfer, so
// // stop my re-transmission of STOP messages for all regencies up to the
// current
// // one
// if (currentRegency > 0) {
// tomLayer.getSynchronizer().removeSTOPretransmissions(currentRegency - 1);
// }
// // if (currentRegency > 0)
// // tomLayer.requestsTimer.setTimeout(tomLayer.requestsTimer.getTimeout() *
// // (currentRegency * 2));

// dt.deliverLock();
// waitingCID = -1;
// dt.update(state);

// if (!appStateOnly && execManager.stopped()) {
// Queue<ConsensusMessage> stoppedMsgs = execManager.getStoppedMsgs();
// for (ConsensusMessage stopped : stoppedMsgs) {
// if (stopped.getNumber() > state.getLastCID() /* msg.getCID() */) {
// execManager.addOutOfContextMessage(stopped);
// }
// }
// execManager.clearStopped();
// execManager.restart();
// }

// tomLayer.processOutOfContext();

// if (SVController.getCurrentViewId() != currentView.getId()) {
// logger.info("Installing current view!");
// SVController.reconfigureTo(currentView);
// }

// isInitializing = false;

// dt.canDeliver();
// dt.deliverUnlock();

// reset();

// logger.info("I updated the state!");

// tomLayer.requestsTimer.Enabled(true);
// tomLayer.requestsTimer.startTimer();
// if (stateTimer != null) {
// stateTimer.cancel();
// }

// if (appStateOnly) {
// appStateOnly = false;
// tomLayer.getSynchronizer().resumeLC();
// }
// } else if ((SVController.getCurrentViewN() / 2) < getReplies()) {
// logger.info("HEY HEY HEY");
// waitingCID = -1;
// reset();

// if (stateTimer != null) {
// stateTimer.cancel();
// }

// if (appStateOnly) {
// requestState();
// }
// } else if (state == null) {
// logger.info(
// "The replica from which I expected the state, sent one which doesn't match
// the hash of the others, or it never sent it at all");

// reset();
// requestState();

// if (stateTimer != null) {
// stateTimer.cancel();
// }
// } else if ((SVController.getCurrentViewN() - SVController.getCurrentViewF())
// <= getReplies()) {

// logger.info("Could not obtain the state, retrying");
// reset();
// if (stateTimer != null) {
// stateTimer.cancel();
// }
// waitingCID = -1;
// } else {
// logger.info("State transfer not yet finished");
// }
// }
// }
// }
// lockTimer.unlock();
// }

// private int[] getStateSize(int parts, int stateSize) {
// int localRange = (stateSize - 1) / parts + 1;
// int spare = localRange * parts - stateSize;
// int currentIndex = 0;
// int[] tasksSize = new int[parts];

// for (int i = 0; i < parts; i++) {
// final int min = currentIndex;
// final int max = min + localRange - (i < spare ? 1 : 0);
// tasksSize[i] = max - min;
// currentIndex = max;
// }

// return tasksSize;
// }

// }
