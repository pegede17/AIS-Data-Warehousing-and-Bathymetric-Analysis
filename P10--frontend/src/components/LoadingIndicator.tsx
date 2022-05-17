import React from 'react';
import {ScaleLoader} from "react-spinners";

const LoadingIndicator: React.FC<{ status: boolean, message: string }> = ({status, message}) => {
    if (status) {
        return <div className={'d-flex align-items-center justify-content-center'}>
            <ScaleLoader color={"#4f7ffe"} height={15} width={5}/>
            <span className={'mx-2'}>{message}</span>
        </div>
    }

    return null;
};

export default LoadingIndicator;
