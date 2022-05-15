package bftsmart.statemanagement.collaborative;

import java.util.LinkedList;
import java.util.Set;

import bftsmart.consensus.messages.ConsensusMessage;
import bftsmart.reconfiguration.ServerViewController;
import bftsmart.statemanagement.ApplicationState;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.leaderchange.CertifiedDecision;
import bftsmart.tom.server.defaultservices.CommandsInfo;
import bftsmart.tom.util.BatchBuilder;

public class CollaborativeState implements ApplicationState {

    protected byte[] state; // If N > 1, it is only part of the State
    protected int lastCID = -1;

    private CommandsInfo[] messageBatches;
    private int lastCheckpointCID;

    private int pid;

    public CollaborativeState(byte[] state, int lastCID, CommandsInfo[] messageBatches, int lastCheckpointCID,
            int pid) {
        this.state = state;
        this.lastCID = lastCID;
        this.messageBatches = messageBatches;
        this.lastCheckpointCID = lastCheckpointCID;
        this.pid = pid;
    }

    @Override
    public int getLastCID() {
        return this.lastCID;
    }

    @Override
    public CertifiedDecision getCertifiedDecision(ServerViewController controller) {
        CommandsInfo ci = getMessageBatch(getLastCID());
        if (ci != null && ci.msgCtx[0].getProof() != null) { // do I have a proof for the consensus?
            Set<ConsensusMessage> proof = ci.msgCtx[0].getProof();
            LinkedList<TOMMessage> requests = new LinkedList<>();

            // Recreate all TOMMessages ordered in the consensus
            for (int i = 0; i < ci.commands.length; i++) {
                requests.add(ci.msgCtx[i].recreateTOMMessage(ci.commands[i]));
            }

            // Serialize the TOMMessages to re-create the proposed value
            BatchBuilder bb = new BatchBuilder(0);
            byte[] value = bb.makeBatch(requests, ci.msgCtx[0].getNumOfNonces(),
                    ci.msgCtx[0].getSeed(), ci.msgCtx[0].getTimestamp(),
                    controller.getStaticConf().getUseSignatures() == 1);

            // Assemble and return the certified decision
            return new CertifiedDecision(pid, getLastCID(), value, proof);
        } else
            return null;
    }

    @Override
    public boolean hasState() {
        return this.state != null;
    }

    @Override
    public void setSerializedState(byte[] state) {
        this.state = state;
    }

    @Override
    public byte[] getSerializedState() {
        return this.state;
    }

    @Override
    public byte[] getStateHash() {
        // Not caring about BFT right now
        return null;
    }

    public CommandsInfo[] getMessageBatches() {
        return this.messageBatches;
    }

    public CommandsInfo getMessageBatch(int cid) {
        if (this.messageBatches != null && cid >= lastCheckpointCID && cid <= lastCID) {
            return this.messageBatches[cid - lastCheckpointCID - 1];
        } else
            return null;
    }

    public void setMessageBatches(CommandsInfo[] messageBatches) {
        this.messageBatches = messageBatches;
    }

    public int getLastCheckpointCID() {
        return this.lastCheckpointCID;
    }

    public int getPid() {
        return this.pid;
    }
}
