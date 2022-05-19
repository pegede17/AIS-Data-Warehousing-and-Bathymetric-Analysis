import React, {createContext, useContext, useReducer} from "react";
import {ToastActions, ToastActionTypes, ToastState} from "../models/Toast";

const ToastStateContext = createContext<{
    state: ToastState;
    dispatch: React.Dispatch<any>;
}>({
    state: {toasts: []},
    dispatch: () => null
});

const ToastReducer = (state: ToastState, action: ToastActions) => {
    switch (action.type) {
        case ToastActionTypes.ADD_TOAST:
            return {
                ...state,
                toasts: [...state.toasts, action.toast],
            }
        case ToastActionTypes.DELETE_TOAST: {
            const updatedToasts = state.toasts.filter((e) => e.id !== action.id);
            return {
                ...state,
                toasts: updatedToasts
            }
        }
    }
}

const ToastProvider: React.FC = ({children}) => {
    const [state, dispatch] = useReducer(ToastReducer, {
        toasts: [],
    });

    return (
        <ToastStateContext.Provider value={{state, dispatch}}>
            {children}
        </ToastStateContext.Provider>
    )
}

export {ToastProvider, ToastStateContext}
// export const useToastStateContext = () => useContext(ToastStateContext);
