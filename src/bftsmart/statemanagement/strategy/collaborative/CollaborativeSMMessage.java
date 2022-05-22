package bftsmart.statemanagement.strategy.collaborative;

import bftsmart.reconfiguration.views.View;
import bftsmart.statemanagement.ApplicationState;
import bftsmart.statemanagement.SMMessage;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class CollaborativeSMMessage extends SMMessage {

    private int[] whichPart;

    public CollaborativeSMMessage() {
        super();
    }

    public CollaborativeSMMessage(int sender, int cid, int type, ApplicationState state, View view, int regency,
                                  int leader, int[] whichPart) {
        super(sender, cid, type, state, view, regency, leader);
        this.whichPart = whichPart;
    }

    public int[] getWhichPart() {
        return whichPart;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(whichPart);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        whichPart = (int[]) in.readObject();
    }
}
