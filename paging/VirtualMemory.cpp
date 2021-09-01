#include "VirtualMemory.h"
#include "PhysicalMemory.h"


struct evictVals {
    uint64_t physicalAddressParent;
    uint64_t frameIndex;
    int weightFromRoot;
    int pageIndex;
} ;

void findLeafInPageTree(uint64_t virtualAddress, int *offSet, word_t *frameIndex);


int addToWeight(int weightFromRoot, int index);

bool checkPageTreeForNextFrame(int curFrame, word_t *frameIndexToReturn, word_t prevOriginFrame, word_t* maxFrameIndex,
                               int curDepth, evictVals* curMax, int curWeightFromRoot, int curPageIndex, int parentPhysicalAddress);

void clearTable(uint64_t frameIndex) {
    for (uint64_t i = 0; i < PAGE_SIZE; ++i) {
        PMwrite(frameIndex * PAGE_SIZE + i, 0);
    }
}

void VMinitialize() {
    clearTable(0);
}

bool checkPageTreeForNextFrame(int curFrame, word_t *frameIndexToReturn, word_t prevOriginFrame, word_t* maxFrameIndex,
                               int curDepth, evictVals* curMax, int curWeightFromRoot, int curPageIndex, int parentPhysicalAddress)
{
    if(curDepth==TABLES_DEPTH)
    {
        curWeightFromRoot = addToWeight(curWeightFromRoot, curPageIndex);
        if ((curWeightFromRoot>(*curMax).weightFromRoot)||((curWeightFromRoot==(*curMax).weightFromRoot)&&(curPageIndex<(*curMax).pageIndex)))
        {
            (*curMax).weightFromRoot=curWeightFromRoot;
            (*curMax).pageIndex = curPageIndex;
            (*curMax).physicalAddressParent=parentPhysicalAddress;
            (*curMax).frameIndex = curFrame;
        }
        return false;
    }
    bool var = true;
    for (uint64_t j = 0; j < PAGE_SIZE; ++j)
    {
        word_t referenceFromParent;
        PMread(curFrame * PAGE_SIZE + j, &referenceFromParent);
        if(referenceFromParent != 0)
        {
            var = false;
            if ((*maxFrameIndex)<referenceFromParent)
            {
                *maxFrameIndex = referenceFromParent;
            }
            if(checkPageTreeForNextFrame(referenceFromParent, frameIndexToReturn, prevOriginFrame, maxFrameIndex,
                                         curDepth + 1, curMax, addToWeight(curWeightFromRoot,referenceFromParent), curPageIndex*PAGE_SIZE + j,
                                         curFrame * PAGE_SIZE + j))
            {
                return true;
            }
        }
    }
    // a frame with no children and is not the root frame was found
    if ((var)&&(curFrame!=0)&&(curFrame!=prevOriginFrame))
    {
        clearTable(curFrame);
        *frameIndexToReturn = curFrame;
        PMwrite(parentPhysicalAddress, 0); //write 0 where there is a reference to this frame from a different frame
        return true;
    }
    return false;
}
//if the index is odd, add to weightFromRoot the ODD_WEIGHT and otherwise add to it the EVEN_WEIGHT
int addToWeight(int weightFromRoot, int index)
{
    if (index % 2 == 0)
    {
        weightFromRoot+= WEIGHT_EVEN;
    }
    else
    {
        weightFromRoot+= WEIGHT_ODD;
    }
    return weightFromRoot;
}
//find the next available frame that could be used
word_t findNewFrame(word_t prevFrame)
{
    word_t frameToReturn =0;
    word_t curMaxFrame=0;
    evictVals curMax = {0, 0, 0,0};
    if(checkPageTreeForNextFrame(0, &frameToReturn, prevFrame, &curMaxFrame, 0, &curMax, 0, 0, 0))
    {
        return frameToReturn;
    }
    else if (curMaxFrame+1<NUM_FRAMES)
    {
        return curMaxFrame+1;
    }
    else
    {
        PMevict(curMax.frameIndex,curMax.pageIndex);
        PMwrite(curMax.physicalAddressParent,0); // to do - how to know the parent
        return curMax.frameIndex;
    }
}


int VMread(uint64_t virtualAddress, word_t* value) {
    //to find num without offset
    int offSet;
    if(virtualAddress>=VIRTUAL_MEMORY_SIZE)
    {
        return 0;
    }
    word_t frameIndex=0;
    findLeafInPageTree(virtualAddress, &offSet, &frameIndex);
    frameIndex = frameIndex<<OFFSET_WIDTH;
    PMread(frameIndex+offSet,value);
    return 1;
}

//get to the leaf of the virtual address in the page tree
void findLeafInPageTree(uint64_t virtualAddress, int *offSet, word_t *frameIndex)
{
    //return (((1 << k) - 1) & (number >> (p - 1)));
    (*offSet) = virtualAddress & ((1 << OFFSET_WIDTH)-1);
    int pageNum = virtualAddress >> OFFSET_WIDTH;
    int pageNumToManipulate = pageNum;
    word_t parentFrameIndex=0;
    int i=1;
    int howMuchToShift;
    int numOfRows;
    if (TABLES_DEPTH==0)
    {
        numOfRows = 0;
    }
    else
    {
        numOfRows = (VIRTUAL_ADDRESS_WIDTH-OFFSET_WIDTH)/
                TABLES_DEPTH;
        if(numOfRows*TABLES_DEPTH<VIRTUAL_ADDRESS_WIDTH-OFFSET_WIDTH)
        {
            numOfRows++;
        }
    }

    while(i<=TABLES_DEPTH) //in each iteration a frame from layer i is written in his parent\read from his parent
    {
        howMuchToShift = numOfRows*(TABLES_DEPTH-i);
        int curBits  = pageNumToManipulate>>howMuchToShift; // shift the page num to the left until we isolate the relevant bits
        PMread(parentFrameIndex * PAGE_SIZE + curBits, &(*frameIndex));
        if(*frameIndex == 0)
        {
            *frameIndex = findNewFrame(parentFrameIndex);
            if (i<TABLES_DEPTH)
            {
                clearTable(*frameIndex);
            }
            PMwrite(parentFrameIndex * PAGE_SIZE + curBits, *frameIndex);
            if (i == TABLES_DEPTH)
            {
                PMrestore(*frameIndex,pageNum); //if the leaf was just created, we want to restore the PM to it
            }
        }
        pageNumToManipulate = pageNumToManipulate - (curBits<<howMuchToShift);// remove the leftmost bits of the pageNum number
        parentFrameIndex = *frameIndex;
        i++;
    }
}


int VMwrite (uint64_t virtualAddress, word_t value) {
    int offSet;
    word_t frameIndex=0;
    if(virtualAddress>=VIRTUAL_MEMORY_SIZE)
    {
        return 0;
    }
    findLeafInPageTree(virtualAddress, &offSet, &frameIndex);
    frameIndex = frameIndex<<OFFSET_WIDTH;
    PMwrite(frameIndex+offSet,value);
    return 1;
}
