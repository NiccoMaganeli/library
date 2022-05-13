package bftsmart.statemanagement.collaborative;

import bftsmart.reconfiguration.views.View;
import bftsmart.statemanagement.ApplicationState;
import bftsmart.statemanagement.SMMessage;

public class CollaborativeSMMessage extends SMMessage {

    public CollaborativeSMMessage() {
        super();
    }

    public CollaborativeSMMessage(int sender, int cid, int type, ApplicationState state, View view, int regency,
            int leader) {
        super(sender, cid, type, state, view, regency, leader);
    }
}
